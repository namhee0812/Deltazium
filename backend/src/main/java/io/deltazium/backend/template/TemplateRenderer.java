package io.deltazium.backend.template;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * connectors/*.json.tmpl의 {{var}} 자리를 채워 배포용 커넥터 설정을 만든다.
 * 미해결 placeholder가 남으면 실패 — 잘못된 설정이 Connect까지 가는 것을 막는다.
 */
@Component
public class TemplateRenderer {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{([a-zA-Z0-9_]+)}}");

    private final Path templateDir;

    public TemplateRenderer(@Value("${deltazium.connectors.template-dir}") String templateDir) {
        this.templateDir = Path.of(templateDir);
    }

    /** @param templateName 예: "source", "jdbc-sink" (확장자 제외) */
    public String render(String templateName, Map<String, String> vars) {
        String template = read(templateName);
        Matcher m = PLACEHOLDER.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            String value = vars.get(key);
            if (value == null) {
                throw new IllegalArgumentException(
                        "템플릿 %s: 값이 없는 placeholder {{%s}}".formatted(templateName, key));
            }
            m.appendReplacement(out, Matcher.quoteReplacement(jsonEscape(value)));
        }
        m.appendTail(out);
        return out.toString();
    }

    private String read(String templateName) {
        Path file = templateDir.resolve(templateName + ".json.tmpl");
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException("템플릿 읽기 실패: " + file, e);
        }
    }

    private static String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
