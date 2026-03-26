"""Tests for the firmware_db database connections."""

import os
import pytest
import firmware_db
from sqlalchemy import create_engine, text
def test_connect(testengine):
    with testengine.connect() as conn:
        result=conn.execute(text("SELECT 'engine_works';"))
        res_list = list(result)
        assert len(res_list) == 1
        assert "engine_works" == res_list[0][0]
