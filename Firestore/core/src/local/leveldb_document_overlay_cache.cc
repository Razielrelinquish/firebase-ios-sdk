/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Firestore/core/src/local/leveldb_document_overlay_cache.h"

#include <map>
#include <string>
#include <unordered_set>
#include <utility>

#include "Firestore/Protos/nanopb/firestore/local/document_overlay.nanopb.h"
#include "Firestore/core/src/credentials/user.h"
#include "Firestore/core/src/local/leveldb_key.h"
#include "Firestore/core/src/local/leveldb_persistence.h"
#include "Firestore/core/src/local/local_serializer.h"
#include "Firestore/core/src/nanopb/message.h"
#include "Firestore/core/src/nanopb/reader.h"
#include "Firestore/core/src/util/hard_assert.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"

namespace firebase {
namespace firestore {
namespace local {

using credentials::User;
using model::DocumentKey;
using model::Mutation;
using model::ResourcePath;
using model::mutation::Overlay;
using model::mutation::OverlayHash;
using nanopb::Message;
using nanopb::StringReader;

LevelDbDocumentOverlayCache::LevelDbDocumentOverlayCache(
    const User& user, LevelDbPersistence* db, LocalSerializer* serializer)
    : db_(NOT_NULL(db)),
      serializer_(NOT_NULL(serializer)),
      user_id_(user.is_authenticated() ? user.uid() : "") {
}

absl::optional<Overlay> LevelDbDocumentOverlayCache::GetOverlay(
    const DocumentKey& key) const {
  const std::string leveldb_key = LevelDbDocumentOverlayKey::Key(user_id_, key);

  auto it = db_->current_transaction()->NewIterator();
  it->Seek(leveldb_key);

  if (!(it->Valid() && it->key() == leveldb_key)) {
    return absl::nullopt;
  }

  return ParseOverlay(it->value());
}

void LevelDbDocumentOverlayCache::SaveOverlays(
    int largest_batch_id, const MutationByDocumentKeyMap& overlays) {
  for (const auto& overlays_entry : overlays) {
    SaveOverlay(largest_batch_id, overlays_entry.first, overlays_entry.second);
  }
}

void LevelDbDocumentOverlayCache::RemoveOverlaysForBatchId(int batch_id) {
  const std::string index_prefix_key =
      LevelDbDocumentOverlayLargestBatchIdIndexKey::KeyPrefix(user_id_,
                                                              batch_id);
  auto index_it = db_->current_transaction()->NewIterator();
  for (index_it->Seek(index_prefix_key);
       index_it->Valid() && absl::StartsWith(index_it->key(), index_prefix_key);
       index_it->Next()) {
    LevelDbDocumentOverlayLargestBatchIdIndexKey leveldb_index_key;
    if (!leveldb_index_key.Decode(index_it->key())) {
      HARD_FAIL("LevelDbDocumentOverlayLargestBatchIdIndexKey failed to parse");
    }
    const std::string leveldb_key_str = leveldb_index_key.document_overlays_key();
    LevelDbDocumentOverlayKey leveldb_key;
    if (!leveldb_key.Decode(leveldb_key_str)) {
      HARD_FAIL("LevelDbDocumentOverlayKey failed to parse");
    }

    // Delete the "largest_batch_id" index.
    db_->current_transaction()->Delete(index_it->key());

    // Delete the "collection" index.
    {
      const ResourcePath collection = leveldb_key.document_key().path().PopLast();
      const std::string leveldb_collection_index_key_str = LevelDbDocumentOverlayCollectionIndexKey::Key(user_id_, collection, leveldb_index_key.largest_batch_id(), leveldb_key_str);
      db_->current_transaction()->Delete(leveldb_collection_index_key_str);
    }

    db_->current_transaction()->Delete(leveldb_key_str);
  }
}

DocumentOverlayCache::OverlayByDocumentKeyMap
LevelDbDocumentOverlayCache::GetOverlays(const ResourcePath& collection,
                                         int since_batch_id) const {
  OverlayByDocumentKeyMap result;

  const std::string index_start_key = LevelDbDocumentOverlayCollectionIndexKey::KeyPrefix(user_id_, collection, since_batch_id + 1);
  const std::string index_required_key_prefix = LevelDbDocumentOverlayCollectionIndexKey::KeyPrefix(user_id_, collection);

  auto it = db_->current_transaction()->NewIterator();
  auto index_it = db_->current_transaction()->NewIterator();
  for (index_it->Seek(index_start_key);
       index_it->Valid() && absl::StartsWith(index_it->key(), index_required_key_prefix);
       index_it->Next()) {
    LevelDbDocumentOverlayCollectionIndexKey leveldb_index_key;
    if (!leveldb_index_key.Decode(index_it->key())) {
      HARD_FAIL("LevelDbDocumentOverlayCollectionIndexKey failed to parse");
    }

    const std::string leveldb_key_str = leveldb_index_key.document_overlays_key();
    it->Seek(leveldb_key_str);
    HARD_ASSERT(it->Valid());
    LevelDbDocumentOverlayKey leveldb_key;
    if (! leveldb_key.Decode(it->key())) {
      HARD_FAIL("LevelDbDocumentOverlayKey failed to parse");
    }
    Overlay overlay = ParseOverlay(it->value());
    result.emplace(std::move(leveldb_key).document_key(), std::move(overlay));
  }

  return result;
}

DocumentOverlayCache::OverlayByDocumentKeyMap
LevelDbDocumentOverlayCache::GetOverlays(const std::string& collection_group,
                                         int since_batch_id,
                                         std::size_t count) const {
  // TODO(dconeybe) Implement an index so that this query can be performed
  // without requiring a full table scan.

  std::map<int, std::unordered_set<Overlay, OverlayHash>> overlays_by_batch_id;
  ForEachOverlay([&](absl::string_view, Overlay&& overlay) {
    if (overlay.largest_batch_id() <= since_batch_id) {
      return;
    }
    if (overlay.key().HasCollectionId(collection_group)) {
      overlays_by_batch_id[overlay.largest_batch_id()].emplace(
          std::move(overlay));
    }
  });

  OverlayByDocumentKeyMap result;
  for (auto& overlays_by_batch_id_entry : overlays_by_batch_id) {
    for (auto& overlay : overlays_by_batch_id_entry.second) {
      DocumentKey key = overlay.key();
      result[key] = std::move(overlay);
    }
    if (result.size() >= count) {
      break;
    }
  }

  return result;
}

Overlay LevelDbDocumentOverlayCache::ParseOverlay(
    absl::string_view encoded) const {
  StringReader reader{encoded};
  auto maybe_message =
      Message<firestore_client_DocumentOverlay>::TryParse(&reader);
  auto result = serializer_->DecodeDocumentOverlay(&reader, *maybe_message);
  if (!reader.ok()) {
    HARD_FAIL("DocumentOverlay proto failed to parse: %s",
              reader.status().ToString());
  }

  return result;
}

void LevelDbDocumentOverlayCache::SaveOverlay(int largest_batch_id,
                                              const DocumentKey& key,
                                              const Mutation& mutation) {
  const std::string leveldb_key = LevelDbDocumentOverlayKey::Key(user_id_, key);

  // Delete the index entries for the overlay.
  {
    auto overlay_opt = GetOverlay(key);
    if (overlay_opt) {
      db_->current_transaction()->Delete(LevelDbDocumentOverlayLargestBatchIdIndexKey::Key(user_id_, overlay_opt->largest_batch_id(), leveldb_key));
      db_->current_transaction()->Delete(LevelDbDocumentOverlayCollectionIndexKey::Key(user_id_, key.path().PopLast(), overlay_opt->largest_batch_id(), leveldb_key));
    }
  }

  // Put the overlay into the database.
  {
    Overlay overlay(largest_batch_id, mutation);
    auto serialized_overlay = serializer_->EncodeDocumentOverlay(overlay);
    db_->current_transaction()->Put(leveldb_key, serialized_overlay);
  }

  // Insert the index entries for the overlay.
  {
    db_->current_transaction()->Put(LevelDbDocumentOverlayLargestBatchIdIndexKey::Key(user_id_, largest_batch_id, leveldb_key), "");
    db_->current_transaction()->Put(LevelDbDocumentOverlayCollectionIndexKey::Key(user_id_, key.path().PopLast(), largest_batch_id, leveldb_key), "");
  }
}

void LevelDbDocumentOverlayCache::ForEachOverlay(
    std::function<void(absl::string_view, model::mutation::Overlay&&)> callback)
    const {
  auto it = db_->current_transaction()->NewIterator();
  const std::string user_key = LevelDbDocumentOverlayKey::KeyPrefix(user_id_);
  it->Seek(user_key);
  for (; it->Valid() && absl::StartsWith(it->key(), user_key); it->Next()) {
    callback(it->key(), ParseOverlay(it->value()));
  }
}

}  // namespace local
}  // namespace firestore
}  // namespace firebase
