package io.deltazium.backend.template;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemplateRendererTest {

    @TempDir
    Path dir;

    TemplateRenderer renderer;

    @BeforeEach
    void setUp() throws IOException {
        Files.writeString(dir.resolve("demo.json.tmpl"), """
                { "name": "{{name}}", "config": { "connection.password": "{{password}}" } }""");
        renderer = new TemplateRenderer(dir.toString());
    }

    @Test
    void placeholder를_채운다() throws Exception {
        String out = renderer.render("demo", Map.of("name", "src-1", "password", "pw"));
        JsonNode json = new ObjectMapper().readTree(out);
        assertThat(json.get("name").asText()).isEqualTo("src-1");
        assertThat(json.get("config").get("connection.password").asText()).isEqualTo("pw");
    }

    @Test
    void 값에_따옴표가_있어도_JSON이_깨지지_않는다() throws Exception {
        String out = renderer.render("demo", Map.of("name", "s", "password", "p\"w\\d"));
        JsonNode json = new ObjectMapper().readTree(out);
        assertThat(json.get("config").get("connection.password").asText()).isEqualTo("p\"w\\d");
    }

    @Test
    void 값이_없는_placeholder면_실패한다() {
        assertThatThrownBy(() -> renderer.render("demo", Map.of("name", "s")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("password");
    }

    @Test
    void 실제_리포_템플릿_4종이_전부_렌더링된다() throws Exception {
        // repo의 connectors/를 직접 검증 — 템플릿과 렌더러의 placeholder 규칙이 어긋나면 여기서 잡힌다
        TemplateRenderer real = new TemplateRenderer(findRepoConnectors());
        Map<String, String> vars = Map.ofEntries(
                Map.entry("connector_name", "n"), Map.entry("oracle_host", "h"),
                Map.entry("oracle_port", "1521"), Map.entry("oracle_user", "u"),
                Map.entry("oracle_password", "p"), Map.entry("oracle_dbname", "XE"),
                Map.entry("topic_prefix", "dz"), Map.entry("table_include_list", "SRC.T1"),
                Map.entry("kafka_bootstrap", "localhost:9092"),
                Map.entry("topics", "t1"), Map.entry("recovery_topics", "r1"),
                Map.entry("target_jdbc_url", "jdbc:oracle:thin:@h:1521/XE"),
                Map.entry("target_user", "u"), Map.entry("target_password", "p"),
                Map.entry("collection_name_format", "TGT.${topic}"),
                Map.entry("catalog_jdbc_url", "jdbc:postgresql://localhost:5433/iceberg_catalog"),
                Map.entry("catalog_jdbc_user", "u"), Map.entry("catalog_jdbc_password", "p"),
                Map.entry("warehouse", "s3://deltazium-warehouse/warehouse"),
                Map.entry("s3_endpoint", "http://localhost:9010"),
                Map.entry("s3_access_key", "ak"), Map.entry("s3_secret_key", "sk"),
                Map.entry("iceberg_tables", "changelog.src_t1"));
        ObjectMapper json = new ObjectMapper();
        for (String t : new String[] {"source", "jdbc-sink", "iceberg-sink", "recovery-sink"}) {
            JsonNode node = json.readTree(real.render(t, vars));
            assertThat(node.get("name")).isNotNull();
            assertThat(node.get("config").get("connector.class").asText()).isNotEmpty();
        }
    }

    private static String findRepoConnectors() {
        // gradle 테스트 실행 위치는 backend/ — 루트의 connectors/를 찾는다
        Path p = Path.of("..", "connectors");
        assertThat(Files.isDirectory(p)).as("connectors/ 디렉터리").isTrue();
        return p.toString();
    }
}
