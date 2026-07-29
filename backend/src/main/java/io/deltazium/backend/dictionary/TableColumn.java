package io.deltazium.backend.dictionary;

/** 딕셔너리에서 조회한 컬럼 정보. */
public record TableColumn(String name, String dataType, boolean pk) {
}
