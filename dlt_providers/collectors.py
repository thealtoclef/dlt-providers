from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Dict, Optional

from dlt.common.runtime.collector import Collector
from opentelemetry import metrics
from opentelemetry.metrics import Counter, Histogram, UpDownCounter


class OtelCollector(Collector):
    """A Collector that sends metrics to OpenTelemetry"""

    def __init__(
        self, meter_name: str = "dlt.pipeline", update_interval: float = 1.0
    ) -> None:
        """
        Initialize OtelCollector with OpenTelemetry meter

        Args:
            meter_name (str): Name for the meter instance
            update_interval (float): Interval in seconds for sending metrics updates
        """
        self.meter = metrics.get_meter(meter_name)
        self.update_interval = update_interval
        self.counters: Dict[str, Counter] = {}
        self.histograms: Dict[str, Histogram] = {}
        self.last_values: Dict[str, int] = defaultdict(int)
        self.step_name: Optional[str] = None

    def update(
        self,
        name: str,
        inc: int = 1,
        total: int = None,
        inc_total: int = None,
        message: str = None,
        label: str = None,
    ) -> None:
        """
        Update metrics in OpenTelemetry

        Creates or updates counters and histograms for the given metrics
        """
        counter_key = f"{name}_{label}" if label else name

        # Create counter if it doesn't exist
        if counter_key not in self.counters:
            self.counters[counter_key] = self.meter.create_counter(
                name=f"dlt.{counter_key}.count",
                description=f"Counter for {name}",
                unit="1",
            )
            self.histograms[counter_key] = self.meter.create_histogram(
                name=f"dlt.{counter_key}.value",
                description=f"Histogram for {name}",
                unit="1",
            )

        # Update metrics with attributes
        attributes = {
            "step": self.step_name,
            "label": label if label else "",
            "message": message if message else "",
        }

        if total:
            attributes["total"] = total

        # Record metrics
        self.counters[counter_key].add(inc, attributes)
        self.histograms[counter_key].record(
            self.last_values[counter_key] + inc, attributes
        )

        # Update last known value
        self.last_values[counter_key] += inc

    def _start(self, step: str) -> None:
        """Start collecting metrics for a new step"""
        self.step_name = step
        self.last_values.clear()

    def _stop(self) -> None:
        """Stop collecting metrics for the current step"""
        # Final update of metrics before stopping
        for counter_key, value in self.last_values.items():
            if value > 0:
                self.histograms[counter_key].record(
                    value, {"step": self.step_name, "final": True}
                )

        self.step_name = None
        self.last_values.clear()
