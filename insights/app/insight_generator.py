import functools
import inspect
import json
import logging
import os
from datetime import date
from datetime import datetime
from typing import List
from typing import Optional

import anthropic

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "Eres un analista de datos experto en precios de productos de supermercado en Espana. "
    "Tu tarea es generar un boletin informativo (newsletter) en espanol "
    "basado en los datos de precios proporcionados.\n\n"
    "El boletin debe incluir:\n"
    "1. Un saludo breve y profesional\n"
    "2. Un resumen de los productos con mayores incrementos de precio en lo que va del ano, "
    "incluyendo el porcentaje de variacion\n"
    "3. Un analisis por categorias de productos\n"
    "4. Recomendaciones para el consumidor\n"
    "5. Una despedida firmada como 'Vanta Data Analytics'\n\n"
    "Formato: usa texto plano con **negritas** para resaltar datos importantes. "
    "Mantén un tono informativo pero accesible."
)


def audit_run(audit_dir: str = "data/audit", exclude_params: Optional[List[str]] = None):
    exclude = set(exclude_params or [])

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
            run_dir = os.path.join(audit_dir, func.__name__, timestamp)
            os.makedirs(run_dir, exist_ok=True)

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            params = {k: v for k, v in bound.arguments.items() if k not in exclude}

            input_path = os.path.join(run_dir, "input.json")
            try:
                with open(input_path, "w", encoding="utf-8") as f:
                    json.dump(params, f, ensure_ascii=False, indent=2, cls=DateEncoder)
                logger.info("Audit input saved to %s", input_path)
            except Exception:
                logger.exception("Failed to save audit input to %s", input_path)

            result = func(*args, **kwargs)

            if result is not None:
                output_path = os.path.join(run_dir, "output.txt")
                try:
                    content = result if isinstance(result, str) else serialize_data(result)
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    logger.info("Audit output saved to %s", output_path)
                except Exception:
                    logger.exception("Failed to save audit output to %s", output_path)

            return result

        return wrapper

    return decorator


@audit_run(exclude_params=["api_key"])
def generate_newsletter(data: list, api_key: str, model: str = "claude-sonnet-4-20250514") -> str:
    logger.info("Generating newsletter with %d data rows using model %s", len(data), model)
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model=model,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    "Genera un boletin informativo basado en los siguientes datos de precios de productos:\n\n"
                    + serialize_data(data)
                ),
            }
        ],
    )
    newsletter_text = message.content[0].text
    logger.info("Newsletter generated successfully (%d characters)", len(newsletter_text))
    return newsletter_text


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


def serialize_data(data: list[dict]) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, indent=2, cls=DateEncoder)

    except (TypeError, ValueError) as e:
        logger.error(f"Error serializing data: {data}")
        logger.exception(e)
        raise
