# QueueGuard

QueueGuard는 주문이나 예약 요청이 한꺼번에 몰릴 때, 정해둔 속도와 정원에 맞춰 사용자를 입장시키는 대기열 서비스다. 사용자는 대기 상태를 확인하고, 승인되면 입장권으로 주문이나 예약을 진행한다. Kotlin과 Spring Boot로 구현했으며 대기 상태와 입장권은 Redis에 저장한다.

## 로컬 실행

Java 21, Gradle 9.8.2와 Redis가 필요하다. 저장소 루트에서 실행한다. 아래 명령은 리눅스의 `/dev/urandom`으로 로컬 서비스 토큰을 생성한다. 기존 `.env`가 있으면 그 값을 사용한다.

```bash
if [ ! -f .env ]; then
  (
    umask 077
    printf 'QUEUEGUARD_COMMERCE_TOKEN=%s\n' \
      "$(od -An -N32 -tx1 -v /dev/urandom | tr -d '[:space:]')" > .env
  )
fi

set -a
. ./.env
set +a
```

### 리눅스에 설치한 Redis 사용

Redis가 로컬의 기본 포트 6379에서 실행 중이면 다음 명령으로 QueueGuard를 시작한다.

```bash
redis-cli -p 6379 ping
REDIS_PORT=6379 SERVER_ADDRESS=127.0.0.1 gradle bootRun
```

### Docker Compose로 Redis 실행

```bash
POSTGRES_PASSWORD=unused-for-redis docker compose up -d --wait redis
REDIS_PORT=16390 SERVER_ADDRESS=127.0.0.1 gradle bootRun
```

Compose가 설정 파일을 읽을 때 PostgreSQL 환경변수도 확인한다. 이 명령은 Redis 서비스만 기동하므로 위 값은 PostgreSQL에 쓰이지 않는다. QueueGuard는 Ctrl+C로 종료한다. Compose로 띄운 Redis는 `POSTGRES_PASSWORD=unused-for-redis docker compose down redis`로 정리한다.

### 동작 확인

다른 터미널에서 서버 상태를 확인한다. 정상 실행 중이면 `status`가 `UP`으로 나온다.

```bash
curl -fsS http://127.0.0.1:18090/actuator/health
```

## 다른 프로젝트에 연결

연동할 백엔드에 QueueGuard 주소, 큐 ID, 서비스 토큰을 설정한다. 로컬 기본값은 주소 `http://127.0.0.1:18090`, 큐 ID `commerce-orders`다. 큐 ID를 바꾸려면 QueueGuard의 `QUEUEGUARD_QUEUE_ID`와 연동 백엔드 설정에 같은 값을 사용한다. 서비스 토큰은 QueueGuard의 `QUEUEGUARD_COMMERCE_TOKEN`과 맞춘다.

브라우저는 자기 서비스의 API를 호출한다. 백엔드는 인증된 사용자 ID를 `subjectId`로 사용하고, QueueGuard 호출에 `Authorization: Bearer <서비스 토큰>`을 붙인다. 서비스 토큰은 백엔드에 보관한다.

1. 구매 버튼을 누르면 Gateway나 백엔드에서 `POST /v1/queues/{queueId}/entries`에 `{"subjectId":"user-123"}`을 보낸다.
2. 응답이 `WAITING`이면 `pollAfterMillis` 간격으로 `GET /v1/queues/{queueId}/subjects/{subjectId}/entry`를 조회한다. `waitingAhead`로 앞에 있는 인원을 표시한다.
3. `ADMITTED`가 되면 구매 요청에 `admissionId`와 `admissionToken`을 담는다. 예를 들어 이커머스 연동은 주문 POST의 `X-Queue-Admission-Id`와 `X-Queue-Admission-Token` 헤더를 사용한다.
4. 주문 백엔드는 `POST /v1/queues/{queueId}/admissions/{admissionId}/claim`을 호출한다. 본문에 `subjectId`, `admissionToken`, `requestId`, `requestFingerprint`를 담는다. `CLAIMED`와 유효 기한을 확인한 뒤 주문을 처리한다.
5. 주문 결과를 DB에 저장한 뒤 같은 경로의 `claim`을 `release`로 바꿔 호출한다. 반환 요청의 본문에는 `subjectId`, `admissionToken`, `requestId`를 담는다.

`requestId`는 한 구매 시도에 부여한 고정 ID다. `requestFingerprint`는 백엔드에서 일관된 형식으로 만든 주문 본문의 SHA-256 해시이며 소문자 16진수 64자로 보낸다. 같은 구매를 재시도할 때는 두 값과 주문 내용을 유지한다. 입장권이 만료되면 다시 대기열에 등록하고, 주문 중복은 연동 백엔드의 멱등성 처리로 막는다.
