package com.queueguard.api

import jakarta.servlet.http.HttpServletResponse
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.ConstraintViolationException
import org.slf4j.LoggerFactory
import org.springframework.dao.DataAccessException
import org.springframework.http.HttpStatus
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.InvalidMediaTypeException
import org.springframework.http.ResponseEntity
import org.springframework.http.converter.HttpMessageNotReadableException
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.method.annotation.HandlerMethodValidationException
import org.springframework.web.servlet.resource.NoResourceFoundException
import org.springframework.web.HttpRequestMethodNotSupportedException
import org.springframework.web.HttpMediaTypeNotSupportedException
import org.springframework.web.HttpMediaTypeNotAcceptableException
import org.springframework.web.bind.ServletRequestBindingException
import tools.jackson.databind.ObjectMapper

enum class ErrorCode(val status: HttpStatus, val message: String) {
    INVALID_REQUEST(HttpStatus.BAD_REQUEST, "Invalid request"),
    UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "Service authentication required"),
    FORBIDDEN(HttpStatus.FORBIDDEN, "Access denied"),
    NOT_FOUND(HttpStatus.NOT_FOUND, "Resource not found"),
    CONFLICT(HttpStatus.CONFLICT, "Request conflicts with current state"),
    EXPIRED(HttpStatus.GONE, "Admission or entry expired"),
    CAPACITY_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, "Capacity exceeded"),
    UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "Queue service unavailable"),
    METHOD_NOT_ALLOWED(HttpStatus.METHOD_NOT_ALLOWED, "Method not allowed"),
    NOT_ACCEPTABLE(HttpStatus.NOT_ACCEPTABLE, "Response media type not acceptable"),
    UNSUPPORTED_MEDIA_TYPE(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Unsupported media type"),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error"),
}

data class ApiError(val errorCode: String, val message: String)
class ApiException(val code: ErrorCode) : RuntimeException(code.message)

fun writeApiError(response: HttpServletResponse, mapper: ObjectMapper, code: ErrorCode) {
    response.status = code.status.value()
    response.contentType = "application/json"
    response.characterEncoding = "UTF-8"
    response.setHeader("Cache-Control", "no-store")
    if (code == ErrorCode.UNAUTHORIZED) response.setHeader("WWW-Authenticate", "Bearer")
    mapper.writeValue(response.outputStream, ApiError(code.name, code.message))
}

@RestControllerAdvice
class ApiExceptionHandler {
    private val logger = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(ApiException::class)
    fun business(e: ApiException): ResponseEntity<ApiError> = error(e.code, HttpHeaders().apply {
        if (e.code == ErrorCode.CAPACITY_EXCEEDED) set("Retry-After", "2")
    })

    @ExceptionHandler(
        MethodArgumentNotValidException::class, HandlerMethodValidationException::class,
        ConstraintViolationException::class, HttpMessageNotReadableException::class,
        ServletRequestBindingException::class,
    )
    fun invalid(): ResponseEntity<ApiError> = error(ErrorCode.INVALID_REQUEST)

    @ExceptionHandler(NoResourceFoundException::class)
    fun notFound(): ResponseEntity<ApiError> = error(ErrorCode.NOT_FOUND)

    @ExceptionHandler(HttpRequestMethodNotSupportedException::class)
    fun method(e: HttpRequestMethodNotSupportedException): ResponseEntity<ApiError> =
        error(ErrorCode.METHOD_NOT_ALLOWED, e.headers)

    @ExceptionHandler(HttpMediaTypeNotAcceptableException::class)
    fun accept(e: HttpMediaTypeNotAcceptableException, request: HttpServletRequest): ResponseEntity<ApiError> {
        try {
            MediaType.parseMediaTypes(request.getHeaders(HttpHeaders.ACCEPT).toList())
        } catch (_: InvalidMediaTypeException) {
            return error(ErrorCode.INVALID_REQUEST)
        }
        return error(ErrorCode.NOT_ACCEPTABLE, e.headers)
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException::class)
    fun contentType(): ResponseEntity<ApiError> = error(ErrorCode.UNSUPPORTED_MEDIA_TYPE)

    @ExceptionHandler(DataAccessException::class)
    fun redisFailure(): ResponseEntity<ApiError> = error(ErrorCode.UNAVAILABLE)

    @ExceptionHandler(Exception::class)
    fun unexpected(e: Exception): ResponseEntity<ApiError> {
        // 예외 본문에는 입력·토큰이 포함될 수 있어 타입만 기록한다.
        logger.error("Request failed: {}", e.javaClass.simpleName)
        return error(ErrorCode.INTERNAL_ERROR)
    }

    private fun error(code: ErrorCode, headers: HttpHeaders = HttpHeaders.EMPTY): ResponseEntity<ApiError> =
        ResponseEntity.status(code.status).headers(headers).contentType(MediaType.APPLICATION_JSON)
            .header("Cache-Control", "no-store")
            .body(ApiError(code.name, code.message))
}
