package io.deltazium.backend.registry;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.springframework.http.MediaType;

@WebMvcTest(DbConnectionController.class)
class DbConnectionControllerTest {

    @Autowired
    MockMvc mvc;

    @MockitoBean
    DbConnectionService service;

    @Test
    void 응답에_password가_노출되지_않는다() throws Exception {
        when(service.list()).thenReturn(List.of(new DbConnection(
                1L, "src-dev", "ORACLE", "SOURCE", "h", 1521, "XE", "u", "secret")));

        mvc.perform(get("/api/connections"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("src-dev"))
                .andExpect(jsonPath("$[0].password").doesNotExist());
    }

    @Test
    void db_types는_지원_목록만_내려준다() throws Exception {
        mvc.perform(get("/api/connections/db-types"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].code").value("ORACLE"))
                .andExpect(jsonPath("$[0].label").value("Oracle"));
    }

    @Test
    void 검증_실패는_400과_error_메시지로_돌려준다() throws Exception {
        when(service.create(any())).thenThrow(new IllegalArgumentException("현재 Oracle만 지원한다: MYSQL"));

        mvc.perform(post("/api/connections")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"dbType\":\"MYSQL\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("현재 Oracle만 지원한다: MYSQL"));
    }
}
