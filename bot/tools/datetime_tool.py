import asyncio
import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MONTHS_PT_BR = {
    1: "janeiro",
    2: "fevereiro",
    3: "março",
    4: "abril",
    5: "maio",
    6: "junho",
    7: "julho",
    8: "agosto",
    9: "setembro",
    10: "outubro",
    11: "novembro",
    12: "dezembro",
}

UNITS_PT = {
    0: "zero",
    1: "um",
    2: "dois",
    3: "três",
    4: "quatro",
    5: "cinco",
    6: "seis",
    7: "sete",
    8: "oito",
    9: "nove",
    10: "dez",
    11: "onze",
    12: "doze",
    13: "treze",
    14: "quatorze",
    15: "quinze",
    16: "dezesseis",
    17: "dezessete",
    18: "dezoito",
    19: "dezenove",
}
TENS_PT = {
    20: "vinte",
    30: "trinta",
    40: "quarenta",
    50: "cinquenta",
    60: "sessenta",
    70: "setenta",
    80: "oitenta",
    90: "noventa",
}
HUNDREDS_PT = {
    100: "cem",
    200: "duzentos",
    300: "trezentos",
    400: "quatrocentos",
    500: "quinhentos",
    600: "seiscentos",
    700: "setecentos",
    800: "oitocentos",
    900: "novecentos",
}


def _num_to_pt(n: int) -> str:
    if n < 20:
        return UNITS_PT[n]
    if n < 100:
        d = (n // 10) * 10
        r = n % 10
        return TENS_PT[d] if r == 0 else f"{TENS_PT[d]} e {UNITS_PT[r]}"
    if n < 1000:
        if n == 100:
            return "cem"
        h = (n // 100) * 100
        r = n % 100
        htxt = "cento" if h == 100 else HUNDREDS_PT[h]
        return htxt if r == 0 else f"{htxt} e {_num_to_pt(r)}"
    if n < 10000:
        th = n // 1000
        r = n % 1000
        if th == 1:
            prefix = "mil"
        else:
            prefix = f"{_num_to_pt(th)} mil"
        return prefix if r == 0 else f"{prefix} e {_num_to_pt(r)}"
    return str(n)


async def get_current_datetime(timezone: str = "America/Sao_Paulo") -> dict:
    """Get the current date and time in a specified timezone.

    Args:
        timezone (str): The timezone to get the current date and time in.
                        Defaults to "America/Sao_Paulo".
    Returns:
        dict: A dictionary with 'success' (bool) and either 'result' (str) or 'error' (str).
    """
    try:
        # Simulate a delay or long-running operation if needed for testing timeout
        # await asyncio.sleep(8) 

        tz = ZoneInfo(timezone)
        now = datetime.datetime.now(tz)
        day_pt = _num_to_pt(now.day)
        year_pt = _num_to_pt(now.year)
        hour_pt = _num_to_pt(now.hour)
        minute_pt = _num_to_pt(now.minute)
        spoken_pt_br = (
            f"{day_pt} de {MONTHS_PT_BR[now.month]} de {year_pt}, "
            f"às {hour_pt} horas e {minute_pt} minutos"
        )
        result = {
            "iso": now.isoformat(),
            "human": now.strftime("%d/%m/%Y às %H:%M"),
            "spoken_pt_br": spoken_pt_br,
            "timezone": timezone,
        }
        return {"success": True, "result": result}
    except ZoneInfoNotFoundError:
        return {"success": False, "error": f"Unknown timezone '{timezone}'. Please provide a valid IANA timezone name."}
    except asyncio.TimeoutError:
        return {"success": False, "error": "Tool execution timed out after 7 seconds."}
    except Exception as e:
        return {"success": False, "error": f"An unexpected error occurred: {e}"}

async def _get_current_datetime_with_timeout(timezone: str = "America/Sao_Paulo") -> dict:
    try:
        return await asyncio.wait_for(get_current_datetime(timezone), timeout=7.0)
    except asyncio.TimeoutError:
        return {"success": False, "error": "Tool execution timed out after 7 seconds."}
    except Exception as e:
        return {"success": False, "error": f"An unexpected error occurred: {e}"}

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_current_datetime",
            "description": "Get the current date and time in a specified timezone. Returns a JSON object with 'success' (boolean) and either 'result' (string) on success or 'error' (string) on failure.",
            "parameters": {
                "type": "object",
                "properties": {
                    "timezone": {
                        "type": "string",
                        "description": "The IANA timezone name, e.g., 'America/New_York', 'Europe/London'. Defaults to 'America/Sao_Paulo' if not provided."
                    }
                },
                "required": []
            },
        },
    }
]
