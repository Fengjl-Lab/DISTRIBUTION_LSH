//
// Created by chenjunhao on 2023/11/25.
//

#pragma once

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <iostream>

namespace QALSH {
enum class ExceptionType {
    /** Invalid exception type.*/
    INVALID = 0,
    /** Valur out of range. */
    OUT_OF_RANGE = 1,
    /** Conversion/casting error. */
    CONVERSIONS = 2,
    /** UNKNOWN type in the type subsystem */
    UNKNOWN_TYPE = 3,
    /** Decimal-related errors. */
    DECIMAL = 4,
    /** Type mismatch. */
    MISMATCH_TYPE = 5,
    /** Divide by 0. */
    DIVIDE_BY_ZERO = 6,
    /** Incompatible type. */
    INCOMPATIBLE_TYPE = 8,
    /** Out of memory error */
    OUT_OF_MEMORY = 9,
    /** Method not implemented. */
    NOT_IMPLEMENTED = 11,
    /** Execution exception*/
    EXECUTION = 12,
};

class Exception : public std::runtime_error {
 public:
    /**
     * Construct a new Exception instance.
     * @param message The exception message
     * */
     explicit Exception(const std::string &message, bool print = true)
        : std::runtime_error(message), type_(ExceptionType::INVALID) {
#ifndef NDEBUG
        if (print) {
            std::string exception_message = "Message :: " + message + "\n";
            std::cerr << exception_message;
        }
#endif
     }

     /**
      * Construct a new Exception instance with specified type.
      * @param exception_type the exception tyoe
      * @param message The exception message
      */
      explicit Exception(ExceptionType exception_type, const std::string &message, bool print = true)
        : std::runtime_error(message), type_(exception_type) {
#ifndef NDEBUG
        if (print) {
            std::string exception_message =
                    "\nException Type :: " + ExceptionTypeToString(type_) + "\nMessage :: " + message + "\n";
            std::cerr << exception_message;
        }
#endif
     }

     /** @return The type of the exception* */
     auto GetType() const -> ExceptionType { return type_;}

     /** @return A human-readable string for the specified exception. */
     static auto ExceptionTypeToString(ExceptionType type) -> std::string {
         switch (type) {
             case ExceptionType::INVALID:
                 return "INVALID";
             case ExceptionType::OUT_OF_RANGE:
                 return "Out of Range";
             case ExceptionType::CONVERSIONS:
                 return "CONVERSIONS";
             case ExceptionType::UNKNOWN_TYPE:
                 return "Unknown Type";
             case ExceptionType::DECIMAL:
                 return "Decimal";
             case ExceptionType::MISMATCH_TYPE:
                 return "Mismatch Type";
             case ExceptionType::DIVIDE_BY_ZERO:
                 return "Divide by Zero";
             case ExceptionType::INCOMPATIBLE_TYPE:
                 return "Incompatible type";
             case ExceptionType::OUT_OF_MEMORY:
                 return "Out of Memory";
             case ExceptionType::NOT_IMPLEMENTED:
                 return "Not implemented";
             default:
                 return "Unknown";
         }
     }

 private:
    ExceptionType type_;
};

class NotImplementedException : public Exception {
 public:
    NotImplementedException() = delete;
    explicit NotImplementedException(const std::string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {}
};

class ExecutionException : public Exception {
 public:
    ExecutionException() = delete;
    explicit ExecutionException(const std::string &msg) : Exception(ExceptionType::EXECUTION, msg, false) {}
};

} // namespace QALSH
