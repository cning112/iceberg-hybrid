package com.streamfirst.iceberg.hybrid.domain;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Generic result type that represents either success with data or failure with error.
 * Provides type-safe error handling for operations that can fail.
 * 
 * @param <T> the type of data returned on success
 */
@Value
@EqualsAndHashCode
public class Result<T> {
    
    boolean success;
    T data;
    String errorMessage;
    Optional<String> errorCode;

    private Result(boolean success, T data, String errorMessage, Optional<String> errorCode) {
        this.success = success;
        this.data = data;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
    }

    /**
     * Creates a successful result with data.
     */
    public static <T> Result<T> success(@NonNull T data) {
        return new Result<>(true, data, null, Optional.empty());
    }

    /**
     * Creates a successful result without data (for void operations).
     */
    public static Result<Void> success() {
        return new Result<>(true, null, null, Optional.empty());
    }

    /**
     * Creates a successful result with message.
     */
    public static Result<String> success(String message) {
        return new Result<>(true, message, null, Optional.empty());
    }

    /**
     * Creates a failure result with error message.
     */
    public static <T> Result<T> failure(String errorMessage) {
        return new Result<>(false, null, errorMessage, Optional.empty());
    }

    /**
     * Creates a failure result with error message and code.
     */
    public static <T> Result<T> failure(String errorMessage, String errorCode) {
        return new Result<>(false, null, errorMessage, Optional.of(errorCode));
    }

    /**
     * Returns the data if successful, or throws an exception if failed.
     */
    public T orElseThrow() {
        if (success) {
            return data;
        }
        throw new RuntimeException(errorMessage + errorCode.map(code -> " (code: " + code + ")").orElse(""));
    }

    /**
     * Returns the data if successful, or the provided default value if failed.
     */
    public T orElse(T defaultValue) {
        return success ? data : defaultValue;
    }

    /**
     * Returns the data if successful, or gets it from the provided supplier if failed.
     */
    public T orElseGet(Supplier<T> supplier) {
        return success ? data : supplier.get();
    }

    /**
     * Maps the data to another type if successful, preserves failure if failed.
     */
    public <U> Result<U> map(Function<T, U> mapper) {
        if (success) {
            return Result.success(mapper.apply(data));
        }
        return Result.failure(errorMessage, errorCode.orElse(null));
    }

    /**
     * Flat maps the data to another Result if successful, preserves failure if failed.
     */
    public <U> Result<U> flatMap(Function<T, Result<U>> mapper) {
        if (success) {
            return mapper.apply(data);
        }
        return Result.failure(errorMessage, errorCode.orElse(null));
    }

    /**
     * Returns true if this is a successful result.
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * Returns true if this is a failed result.
     */
    public boolean isFailure() {
        return !success;
    }

    /**
     * Gets the data if successful, empty otherwise.
     */
    public Optional<T> getData() {
        return success ? Optional.ofNullable(data) : Optional.empty();
    }

    /**
     * Gets the error message if failed, empty otherwise.
     */
    public Optional<String> getErrorMessage() {
        return success ? Optional.empty() : Optional.ofNullable(errorMessage);
    }

    @Override
    public String toString() {
        if (success) {
            return "Result.success(" + data + ")";
        } else {
            return "Result.failure(" + errorMessage + 
                   errorCode.map(code -> ", code=" + code).orElse("") + ")";
        }
    }
}