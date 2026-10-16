import json
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError
from spb_transport_contracts import VehicleEtaSnapshotV1, VehiclePositionV1

FIXTURES = Path(__file__).parents[1] / "fixtures"


def load_fixture(name: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads((FIXTURES / name).read_text()))


def test_confirmed_vehicle_position_contract_accepts_zero_speed() -> None:
    position = VehiclePositionV1.model_validate(load_fixture("vehicle-position.v1.json"))

    assert position.speed == 0


def test_vehicle_eta_contract_accepts_empty_forecasts() -> None:
    snapshot = VehicleEtaSnapshotV1.model_validate(load_fixture("vehicle-eta-empty.v1.json"))

    assert snapshot.forecasts == ()


def test_vehicle_eta_rank_is_derived_order_not_gtfs_stop_sequence() -> None:
    snapshot = VehicleEtaSnapshotV1.model_validate(load_fixture("vehicle-eta.v1.json"))

    assert [item.forecast_rank for item in snapshot.forecasts] == [0, 1]


def test_unconfirmed_realtime_fields_are_rejected() -> None:
    payload = load_fixture("vehicle-position.v1.json")
    payload["trip_id"] = "unconfirmed"

    with pytest.raises(ValidationError, match="trip_id"):
        VehiclePositionV1.model_validate(payload)
