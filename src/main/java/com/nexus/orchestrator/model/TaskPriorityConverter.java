package com.nexus.orchestrator.model;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class TaskPriorityConverter implements AttributeConverter<TaskPriority, Short> {

    @Override
    public Short convertToDatabaseColumn(TaskPriority attribute) {
        if (attribute == null) {
            return (short) TaskPriority.NORMAL.getLevel();
        }
        return (short) attribute.getLevel();
    }

    @Override
    public TaskPriority convertToEntityAttribute(Short dbData) {
        if (dbData == null) {
            return TaskPriority.NORMAL;
        }
        return TaskPriority.fromLevel(dbData.intValue());
    }
}
