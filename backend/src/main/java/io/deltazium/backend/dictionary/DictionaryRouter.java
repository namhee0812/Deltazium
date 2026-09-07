package io.deltazium.backend.dictionary;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.deltazium.backend.registry.DbConnection;
import io.deltazium.backend.registry.DbType;
import org.springframework.stereotype.Service;

/**
 * 파일명 : DictionaryRouter.java
 * 작성일자 : 26. 09. 07.
 * 작성자 : 최남희
 * 설명 : DbConnection의 dbType으로 알맞은 SourceDictionary 구현을 고른다
 * (architecture.md 8절 "소스별 분기가 공식적으로 존재하는 유일한 자리"). 소스·타깃 어느
 * 연결이든 이 라우터를 거친다 — 컬럼 조회(columns API)는 타깃에도 쓰이기 때문.
 *
 * <p>
 * 수정 내역
 * --------------------------------------------------
 * 수정일자      | 수정자   | 수정내역
 * --------------------------------------------------
 * 26. 09. 07.       | 최남희  | 최초 생성 — 다중 소스·다중 타깃 ②. byType 맵을 생성자가 아니라
 * |                          | 최초 호출 시점에 조립하도록 지연 — 테스트에서 @MockitoBean으로
 * |                          | 주입한 SourceDictionary는 컨텍스트 초기화 시점엔 dbType()이
 * |                          | 아직 스터빙 전(null)이라 생성자에서 즉시 맵을 만들면 깨진다
 * --------------------------------------------------
 */
@Service
public class DictionaryRouter {

    private final List<SourceDictionary> impls;
    private volatile Map<DbType, SourceDictionary> byType;

    public DictionaryRouter(List<SourceDictionary> impls) {
        this.impls = impls;
    }

    public SourceDictionary forConnection(DbConnection conn) {
        DbType type = DbType.find(conn.dbType())
                .orElseThrow(() -> new IllegalArgumentException("알 수 없는 DB 종류: " + conn.dbType()));
        SourceDictionary d = byType().get(type);
        if (d == null) {
            throw new IllegalArgumentException("딕셔너리 미구현 DB 종류: " + type.label());
        }
        return d;
    }

    private Map<DbType, SourceDictionary> byType() {
        Map<DbType, SourceDictionary> m = byType;
        if (m == null) {
            m = impls.stream().collect(Collectors.toMap(SourceDictionary::dbType, Function.identity()));
            byType = m;
        }
        return m;
    }
}
