@echo off
REM ============================================================
REM Authentication & Security Test Script (Windows)
REM Tests JWT and authentication mechanisms
REM ============================================================

echo ================================================================
echo          AUTHENTICATION ^& SECURITY TEST
echo ================================================================
echo.

set API_GATEWAY=http://localhost:8090

echo ================================================================
echo AUTHENTICATION ANALYSIS
echo ================================================================
echo.
echo Current Implementation: BASIC AUTHENTICATION
echo JWT Implementation: NOT FOUND
echo.
echo The system currently uses Spring Security Basic Authentication
echo instead of JWT tokens. This means:
echo.
echo   - Users authenticate with username/password on each request
echo   - No token-based authentication
echo   - No token expiration mechanism
echo   - No refresh token flow
echo.

echo ================================================================
echo TEST 1: User Registration
echo ================================================================
echo.

set TIMESTAMP=%time:~0,2%%time:~3,2%%time:~6,2%
set TEST_USER=sectest_%TIMESTAMP%
set TEST_EMAIL=sectest_%TIMESTAMP%@example.com

echo Registering user: %TEST_USER%
curl -X POST "%API_GATEWAY%/auth/signup" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"SecurePass@123\", \"email\": \"%TEST_EMAIL%\", \"firstName\": \"Security\", \"lastName\": \"Test\"}"

echo.
echo.

echo ================================================================
echo TEST 2: User Login (Basic Auth)
echo ================================================================
echo.

echo Logging in user: %TEST_USER%
curl -X POST "%API_GATEWAY%/auth/signin" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"SecurePass@123\"}"

echo.
echo.

echo ================================================================
echo TEST 3: Access Protected Endpoint (Without Auth)
echo ================================================================
echo Expected: 401 Unauthorized
echo.

curl -X GET "%API_GATEWAY%/auth/user/%TEST_USER%" ^
  -w "\nHTTP Status: %%{http_code}\n"

echo.
echo.

echo ================================================================
echo TEST 4: Access Protected Endpoint (With Basic Auth)
echo ================================================================
echo.

curl -X GET "%API_GATEWAY%/auth/user/%TEST_USER%" ^
  -u "%TEST_USER%:SecurePass@123" ^
  -w "\nHTTP Status: %%{http_code}\n"

echo.
echo.

echo ================================================================
echo TEST 5: Invalid Credentials
echo ================================================================
echo Expected: 401 Unauthorized
echo.

curl -X POST "%API_GATEWAY%/auth/signin" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"WrongPassword\"}" ^
  -w "\nHTTP Status: %%{http_code}\n"

echo.
echo.

echo ================================================================
echo TEST 6: Duplicate Username Registration
echo ================================================================
echo Expected: 400 Bad Request - Username already exists
echo.

curl -X POST "%API_GATEWAY%/auth/signup" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"AnotherPass@123\", \"email\": \"another@example.com\"}" ^
  -w "\nHTTP Status: %%{http_code}\n"

echo.
echo.

echo ================================================================
echo SECURITY RECOMMENDATIONS
echo ================================================================
echo.
echo CRITICAL FINDINGS:
echo.
echo 1. JWT NOT IMPLEMENTED
echo    - Current: Basic Authentication (username/password per request)
echo    - Recommended: Implement JWT tokens for stateless authentication
echo.
echo 2. MISSING FEATURES:
echo    - No token expiration
echo    - No refresh token mechanism
echo    - No role-based access control enforcement
echo    - No rate limiting
echo    - No account lockout after failed attempts
echo.
echo 3. SECURITY IMPROVEMENTS NEEDED:
echo    - Implement JWT with access and refresh tokens
echo    - Add token expiration (15 min for access, 7 days for refresh)
echo    - Implement role-based authorization
echo    - Add rate limiting for login attempts
echo    - Implement account lockout mechanism
echo    - Add password strength validation
echo    - Implement HTTPS in production
echo.

echo ================================================================
echo                TEST EXECUTION COMPLETE
echo ================================================================
pause
