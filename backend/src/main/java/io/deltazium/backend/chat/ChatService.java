package io.deltazium.backend.chat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.helpers.BetaToolRunner;
import com.anthropic.models.beta.AnthropicBeta;
import com.anthropic.models.beta.messages.BetaContentBlock;
import com.anthropic.models.beta.messages.BetaMessage;
import com.anthropic.models.beta.messages.BetaStopReason;
import com.anthropic.models.beta.messages.BetaToolUseBlock;
import com.anthropic.models.beta.messages.BetaCacheControlEphemeral;
import com.anthropic.models.beta.messages.MessageCreateParams;
import org.springframework.beans.factory.annotation.Value;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import io.deltazium.backend.assist.LogSearchService;
import io.deltazium.backend.assist.OverviewService;

/**
 * 파일명 : ChatService.java
 * 작성일자 : 26. 08. 12.
 * 작성자 : 최남희
 * 설명 : AI 진단 어시스턴트의 Claude 연동 — 사용자 질문을 받아 Claude API를 tool use로
 * 호출하고, 진행 상황·최종 답변을 SSE로 흘린다. Claude가 우리 API를 직접 호출하는 게
 * 아니라 backend가 같은 프로세스의 서비스(OverviewService·LogSearchService)를 직접
 * 호출해 도구를 실행한다 — 네트워크 왕복 없음.
 *
 * <p>
 * 도구는 읽기 전용 2종(GetOverviewTool·SearchLogsTool)뿐이다. 조치를 실행하는 도구는
 * 의도적으로 두지 않는다 — 시스템 프롬프트가 "조치는 제안만" 하도록 지시하고, 애초에
 * 실행할 수단 자체가 없어야 프롬프트 우회에도 안전하다.
 *
 * <p>
 * ANTHROPIC_API_KEY 미설정 시 클라이언트를 만들지 않고 SSE error 이벤트만 보낸다
 * ({@code deploy/env.sh}가 {@code conf/secrets.env}를 로드 — docs/operations.md 참고).
 * 클라이언트는 지연 초기화하고, 최초 요청에서만 환경변수를 확인한다.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 08. 12.       | 최남희  | 최초 생성
 * 26. 08. 13.       | 최남희  | prompt caching 적용, 모델 설정화(deltazium.chat.model), 왕복별 usage 로깅
 * 26. 08. 20.       | 최남희  | 시스템 프롬프트에 복구 레인 지식 추가 — recovery-job을 외부 시스템으로 오판한 답변의 재발 방지
 * 26. 09. 23.       | 최남희  | 401(UnauthorizedException)은 raw JSON 대신 키 갱신 조치 문구로 — 만료 키로 "Claude 호출 실패"만 떠 원인을 못 찾던 것
 * 26. 10. 26.       | 최남희  | 대화방 저장·이어 하기: 질문/답변 텍스트 저장, 최근 20턴 텍스트 이력을 messages로 전달(buildMessages), room 이벤트
 * --------------------------------------------------
 */
@Service
public class ChatService {

    private static final Logger log = LoggerFactory.getLogger(ChatService.class);

    private static final long MAX_TOKENS = 16000L;
    private static final String STRUCTURED_OUTPUTS_BETA = "structured-outputs-2025-11-13";

    private static final String SYSTEM_PROMPT = """
            너는 Deltazium(Debezium+Kafka 기반 CDC 파이프라인) 운영 진단 어시스턴트다.

            구조: Oracle → Debezium source(LogMiner) → Kafka → JDBC sink(타깃 Oracle 실 적재) \
            + Iceberg sink(changelog 보관). backend는 제어면(control plane)이다.

            복구 레인: recovery-job은 Deltazium 자체 모듈이다(Iceberg changelog scan → \
            envelope 재조립 → 복구 토픽 재발행). 복구 트리거 때만 backend가 프로세스로 \
            기동하며 커넥터가 아니므로 GetOverview에 나오지 않는다. 재발행분을 타깃에 \
            적재하는 recovery-sink 커넥터도 별도로 있다. 토폴로지 화면의 recovery-job \
            "평시 정지" 표기는 실시간 상태가 아니라 정적 안내다.

            진단 절차: 반드시 GetOverview로 시작해 무엇이 이상한지 먼저 특정한 뒤, \
            필요하면 SearchLogs로 해당 대상을 더 파고들어라. 대상을 특정하지 않고 \
            SearchLogs부터 쓰지 마라.

            근거: 모든 주장에는 근거가 있어야 한다 — 어느 도구를 호출해 얻은 어떤 필드인지, \
            로그라면 어느 파일의 몇 번째 줄인지 밝혀라. 근거 없는 단정을 하지 말고, \
            모르면 모른다고 말해라.

            조치: 너는 조치를 제안만 할 수 있다. 실행 수단이 없다 — 무엇을 했다고 말하지 말고, \
            무엇을 해야 하는지만 제안해라.

            상태 해석 — 정지가 정상인 것들: 커넥터 PAUSED와 DDL 승인 대기(DETECTED)는 \
            사람의 결정을 기다리는 정상 상태, recovery-job 미기동과 recovery-sink 정지는 \
            평시 설계 상태다. 어느 것도 고장과 동일하게 취급하지 마라.

            답변 스타일: 한국어로, 결론부터, 간결하게 답하라.""";

    /** 모델에 보내는 이력 상한 — 20턴(user+assistant 40건). 텍스트만, 도구 호출/결과는 제외(docs/internals.md). */
    static final int HISTORY_TURNS = 20;
    private static final int TITLE_MAX = 40;

    private final ObjectMapper objectMapper;
    private final ChatRoomRepository roomRepository;
    private final ExecutorService executor;
    private final String model;
    private volatile AnthropicClient client;

    public ChatService(OverviewService overviewService, LogSearchService logSearchService,
                       ObjectMapper objectMapper, ChatRoomRepository roomRepository,
                       @Value("${deltazium.chat.model:claude-sonnet-5}") String model) {
        this.model = model;
        // 도구 클래스는 Jackson이 기본 생성자로 생성해 생성자 주입이 불가능하다 —
        // ToolBeans 참고(결정 필요로 플래그된 정적 브릿지).
        ToolBeans.overviewService = overviewService;
        ToolBeans.logSearchService = logSearchService;
        ToolBeans.objectMapper = objectMapper;
        this.objectMapper = objectMapper;
        this.roomRepository = roomRepository;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @PreDestroy
    void shutdown() {
        executor.shutdown();
    }

    /** 질문 처리를 전용 executor 스레드로 넘긴다 — 호출자(컨트롤러)는 즉시 emitter를 반환한다. */
    public void streamAnswer(Long roomId, String question, SseEmitter emitter) {
        executor.execute(() -> run(roomId, question, emitter));
    }

    /**
     * 모델에 보낼 messages(텍스트만) 조립 — 순수 함수.
     * history(시간순)에서 최근 {@link #HISTORY_TURNS}턴(40건)만 취하고, user/assistant 교대를 보장한 뒤
     * 이번 질문을 user로 붙인다. 같은 role이 연속되면(답변 실패로 assistant가 빠진 턴) 앞의 메시지를 버리고,
     * 맨 앞이 assistant면 버린다(Messages API는 user로 시작해야 한다).
     */
    static List<ChatMessage> buildMessages(List<ChatMessage> history, String question) {
        int from = Math.max(0, history.size() - HISTORY_TURNS * 2);
        List<ChatMessage> out = new ArrayList<>();
        for (ChatMessage m : history.subList(from, history.size())) {
            if (!out.isEmpty() && out.get(out.size() - 1).role().equals(m.role())) {
                out.remove(out.size() - 1);
            }
            out.add(m);
        }
        while (!out.isEmpty() && !"user".equals(out.get(0).role())) {
            out.remove(0);
        }
        if (!out.isEmpty() && "user".equals(out.get(out.size() - 1).role())) {
            out.remove(out.size() - 1);
        }
        out.add(new ChatMessage("user", question, null));
        return out;
    }

    /** 방 제목 — 질문 앞 40자, 줄바꿈은 공백으로. */
    static String titleOf(String question) {
        String t = question.replaceAll("\\s*[\\r\\n]+\\s*", " ").strip();
        return t.length() > TITLE_MAX ? t.substring(0, TITLE_MAX) : t;
    }

    private void run(Long requestedRoomId, String question, SseEmitter emitter) {
        try {
            AnthropicClient anthropicClient = client();
            if (anthropicClient == null) {
                emitEvent(emitter, "error", Map.of("message",
                        "ANTHROPIC_API_KEY 미설정 — ~/deltazium-runtime/conf/secrets.env 참고"));
                return;
            }

            long roomId = requestedRoomId != null && roomRepository.existsRoom(requestedRoomId)
                    ? requestedRoomId : roomRepository.createRoom(null);
            emitEvent(emitter, "room", Map.of("roomId", roomId));
            // 이력은 이번 질문을 저장하기 전에 읽는다
            List<ChatMessage> history = roomRepository.findRecentMessages(roomId, HISTORY_TURNS * 2);
            roomRepository.insertMessage(roomId, "user", question);
            roomRepository.setTitleIfNull(roomId, titleOf(question));
            roomRepository.touchRoom(roomId);

            MessageCreateParams.Builder builder = MessageCreateParams.builder()
                    .model(model)
                    .maxTokens(MAX_TOKENS)
                    .addBeta(STRUCTURED_OUTPUTS_BETA)
                    // 반복 왕복에서 시스템 프롬프트·도구 정의·누적 히스토리가 매번 재전송된다 —
                    // top-level cache 브레이크포인트(마지막 블록 자동 배치)로 반복분을 캐시 읽기(0.1배)로.
                    .cacheControl(BetaCacheControlEphemeral.builder().build())
                    .system(SYSTEM_PROMPT)
                    .addTool(GetOverviewTool.class)
                    .addTool(SearchLogsTool.class);
            for (ChatMessage m : buildMessages(history, question)) {
                if ("assistant".equals(m.role())) {
                    builder.addAssistantMessage(m.content());
                } else {
                    builder.addUserMessage(m.content());
                }
            }
            // server-side fallback은 Opus/Fable 계열 전용 — 다른 모델에 붙이면 400
            if (model.startsWith("claude-opus") || model.startsWith("claude-fable")) {
                builder.addBeta(AnthropicBeta.SERVER_SIDE_FALLBACK_2026_07_01).fallbacksDefault();
            }
            MessageCreateParams params = builder.build();

            BetaToolRunner runner = anthropicClient.beta().messages().toolRunner(params);

            BetaMessage lastMessage = null;
            int iteration = 0;
            for (BetaMessage message : runner) {
                lastMessage = message;
                logUsage(++iteration, message);
                for (BetaContentBlock block : message.content()) {
                    block.toolUse().ifPresent(toolUse -> emitToolEvent(emitter, toolUse));
                }
            }

            if (lastMessage == null) {
                emitEvent(emitter, "error", Map.of("message", "Claude로부터 응답을 받지 못했다"));
            } else if (lastMessage.stopReason().filter(BetaStopReason.REFUSAL::equals).isPresent()) {
                emitEvent(emitter, "error", Map.of("message", "안전 분류기가 요청을 거절했다"));
            } else {
                String answer = extractText(lastMessage);
                roomRepository.insertMessage(roomId, "assistant", answer);
                roomRepository.touchRoom(roomId);
                emitEvent(emitter, "answer", Map.of("text", answer));
            }
        } catch (Exception e) {
            log.warn("chat 처리 실패", e);
            // 401은 운영자가 바로 조치할 수 있는 원인이라 raw JSON 대신 조치 방법을 보여준다
            String message = e instanceof com.anthropic.errors.UnauthorizedException
                    ? "Claude API 키가 유효하지 않습니다 — ~/deltazium-runtime/conf/secrets.env의 "
                      + "ANTHROPIC_API_KEY를 갱신하고 backend를 재기동하세요 (docs/operations.md)"
                    : "Claude 호출 실패: " + e.getMessage();
            emitEvent(emitter, "error", Map.of("message", message));
        } finally {
            emitEvent(emitter, "done", Map.of());
            emitter.complete();
        }
    }

    /** 왕복별 토큰 사용량 — 비용 분석과 cache 적중 검증용. cache_read가 0이면 캐시가 안 먹은 것. */
    private void logUsage(int iteration, BetaMessage message) {
        try {
            var u = message.usage();
            log.info("chat usage [{}] iter={} input={} cache_read={} cache_write={} output={}",
                    model, iteration, u.inputTokens(),
                    u.cacheReadInputTokens(), u.cacheCreationInputTokens(), u.outputTokens());
        } catch (Exception e) {
            log.debug("usage 로깅 실패 — {}", e.getMessage());
        }
    }

    private String extractText(BetaMessage message) {
        StringBuilder text = new StringBuilder();
        for (BetaContentBlock block : message.content()) {
            block.text().ifPresent(t -> text.append(t.text()));
        }
        return text.toString();
    }

    private void emitToolEvent(SseEmitter emitter, BetaToolUseBlock toolUse) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("name", toolUse.name());
        Object inputSummary;
        try {
            inputSummary = toolUse._input().convert(JsonNode.class);
        } catch (Exception e) {
            inputSummary = null;
        }
        fields.put("input", inputSummary);
        emitEvent(emitter, "tool", fields);
    }

    private void emitEvent(SseEmitter emitter, String type, Map<String, Object> fields) {
        try {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("type", type);
            event.putAll(fields);
            emitter.send(SseEmitter.event().data(objectMapper.writeValueAsString(event), MediaType.APPLICATION_JSON));
        } catch (IOException e) {
            log.debug("SSE 전송 실패(클라이언트 연결 종료로 추정) — {}", e.getMessage());
        }
    }

    /** 지연 초기화 — 키 미설정이면 null을 돌려주고 클라이언트를 만들지 않는다. */
    private AnthropicClient client() {
        AnthropicClient current = client;
        if (current != null) {
            return current;
        }
        String apiKey = System.getenv("ANTHROPIC_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            return null;
        }
        synchronized (this) {
            if (client == null) {
                client = AnthropicOkHttpClient.fromEnv();
            }
            return client;
        }
    }
}
