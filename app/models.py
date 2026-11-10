from datetime import datetime
from typing import List, Optional
from uuid import UUID
from pydantic import BaseModel, Field, field_validator


class AppointmentBase(BaseModel):
    patient_code: str = Field(..., min_length=1, max_length=64, description="Знеособлений код пацієнта")
    doctor: str = Field(..., min_length=1, max_length=128, description="ПІБ або ID лікаря")
    room: str = Field(..., min_length=1, max_length=32, description="Номер кабінету")
    start_time: datetime = Field(..., description="Час початку прийому")
    duration_minutes: int = Field(..., description="Тривалість у хвилинах (від 10 до 120, кратна 5)")
    status: str = Field(..., min_length=1, max_length=32, description="Статус прийому")

    @field_validator("duration_minutes")
    @classmethod
    def validate_duration(cls, v: int) -> int:
        if v < 10 or v > 120:
            raise ValueError("Тривалість прийому повинна бути від 10 до 120 хвилин")
        if v % 5 != 0:
            raise ValueError("Тривалість прийому повинна бути кратною 5 хвилинам")
        return v


class AppointmentCreate(AppointmentBase):
    pass


class AppointmentResponse(AppointmentBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True


class AppointmentListResponse(BaseModel):
    items: List[AppointmentResponse]
    total: int
    limit: int
    offset: int