// FULLY PORTED
//  File.swift
//  
//
//  Created by Ryan Wilson on 2022-01-25.
//

import Foundation

/**
 * A `HTTPSCallableResult` contains the result of calling a `HTTPSCallable`.
 */
@objc(FIRHTTPSCallableResult)
public class HTTPSCallableResult : NSObject {
  /**
   * The data that was returned from the Callable HTTPS trigger.
   *
   * The data is in the form of native objects. For example, if your trigger returned an
   * array, this object would be an NSArray. If your trigger returned a JavaScript object with
   * keys and values, this object would be an NSDictionary.
   */
  public var data: Any

  internal init(data: Any) {
    self.data = data
  }
}


/**
 * A `HTTPSCallable` is reference to a particular Callable HTTPS trigger in Cloud Functions.
 */
@objc(FIRHTTPSCallable)
public class HTTPSCallable : NSObject {
  // MARK: - Private Properties

  // The functions client to use for making calls.
  private let functions: Functions

  // The name of the http endpoint this reference refers to.
  private let name: String

  // MARK: - Public Properties

  /**
   * The timeout to use when calling the function. Defaults to 70 seconds.
   */
  public var timeoutInterval: TimeInterval = 70


  internal init(functions: Functions, name: String) {
    self.functions = functions
    self.name = name
  }

  /**
   * Executes this Callable HTTPS trigger asynchronously.
   *
   * The data passed into the trigger can be any of the following types:
   * * NSNull
   * * NSString
   * * NSNumber
   * * NSArray<id>, where the contained objects are also one of these types.
   * * NSDictionary<NSString, id>, where the values are also one of these types.
   *
   * The request to the Cloud Functions backend made by this method automatically includes a
   * Firebase Installations ID token to identify the app instance. If a user is logged in with
   * Firebase Auth, an auth ID token for the user is also automatically included.
   *
   * Firebase Cloud Messaging sends data to the Firebase backend periodically to collect information
   * regarding the app instance. To stop this, see `Messaging.deleteData()`. It
   * resumes with a new FCM Token the next time you call this method.
   *
   * @param data Parameters to pass to the trigger.
   * @param completion The block to call when the HTTPS request has completed.
   */
  public func call(_ data: Any? = nil, completion: @escaping (HTTPSCallableResult?, Error?) -> Void) {
    functions.callFunction(name: name,
                           withObject: data,
                           timeout: timeoutInterval) { result in
      switch result {
      case .success(let callableResult):
        completion(callableResult, nil)
      case .failure(let error):
        completion(nil, error)
      }
    }
  }

#if compiler(>=5.5) && canImport(_Concurrency)
  /**
   * Executes this Callable HTTPS trigger asynchronously.
   *
   * The request to the Cloud Functions backend made by this method automatically includes a
   * FCM token to identify the app instance. If a user is logged in with Firebase
   * Auth, an auth ID token for the user is also automatically included.
   *
   * Firebase Cloud Messaging sends data to the Firebase backend periodically to collect information
   * regarding the app instance. To stop this, see `Messaging.deleteData()`. It
   * resumes with a new FCM Token the next time you call this method.
   *
   * @param data Parameters to pass to the trigger.
   * @returns The result of the call.
   */
  @available(iOS 15, tvOS 15, macOS 12, watchOS 8, *)
  public func call(_ data: Any? = nil) async throws -> HTTPSCallableResult {
    return try await withCheckedThrowingContinuation({ continuation in
        // TODO(bonus): Use task to handle and cancellation.
      self.call(data) { callableResult, error in
        if let callableResult = callableResult {
          continuation.resume(returning: callableResult)
        } else {
          continuation.resume(throwing: error!)
        }
      }
    })
  }
  #endif  // compiler(>=5.5) && canImport(_Concurrency)
}
