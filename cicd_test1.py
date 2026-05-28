#!/usr/bin/env python3
"""
Mock Weather Dashboard
Prints a formatted weather summary for a list of cities.
"""

import random
from dataclasses import dataclass


@dataclass
class WeatherReading:
    city: str
    temp_c: float
    condition: str
    humidity: int  # percentage

    @property
    def temp_f(self) -> float:
        return (self.temp_c * 9 / 5) + 32


CONDITIONS = ["Sunny", "Cloudy", "Rainy", "Partly Cloudy", "Thunderstorm"]
CITIES = ["New York", "London", "Tokyo", "Sydney", "Berlin", "Mumbai"]


def fetch_mock_weather(city: str) -> WeatherReading:
    """Generate a random weather reading for a city."""
    return WeatherReading(
        city=city,
        temp_c=round(random.uniform(5.0, 35.0), 1),
        condition=random.choice(CONDITIONS),
        humidity=random.randint(30, 95),
    )


def display_dashboard(readings: list[WeatherReading]) -> None:
    """Print a formatted weather table."""
    print("\n" + "=" * 50)
    print("         🌤️  WEATHER DASHBOARD")
    print("=" * 50)
    print(f"{'City':<<12} {'°C':>6} {'°F':>6} {'Humidity':>10} {'Condition':>14}")
    print("-" * 50)
    for r in readings:
        print(f"{r.city:<12} {r.temp_c:>6.1f} {r.temp_f:>6.1f} {r.humidity:>9}% {r.condition:>14}")
    print("=" * 50)


def main() -> None:
    readings = [fetch_mock_weather(city) for city in CITIES]
    display_dashboard(readings)


if __name__ == "__main__":
    main()
