SYSTEM_BASE = """Eres un agente de síntesis financiera especializado en mercados latinoamericanos y norteamericanos (MERVAL, BOVESPA, S&P 500).
Respondes únicamente basándote en la información provista en el contexto.
Si el contexto no tiene información suficiente, lo indicas claramente.
Usas terminología financiera precisa. Respondes en español."""

OUTPUT_INSTRUCTIONS = {
    "resumen": """Genera un resumen ejecutivo estructurado con:
- Situación actual del mercado o activo
- Drivers principales (2-3 factores clave)
- Conclusión de 1 oración
Formato: bullets cortos, máximo 250 palabras.""",

    "señales": """Extrae y lista las señales de inversión presentes en el contexto:
- Para cada activo: nombre, ticker, señal (Compra/Neutral/Venta Parcial/Venta), score si disponible
- Agrupa por mercado
- Incluye breve justificación (1 línea por activo)
Formato: tabla markdown o lista estructurada.""",

    "riesgos": """Identifica y clasifica los riesgos presentes en el contexto:
- Riesgos macro (monetario, cambiario, fiscal)
- Riesgos micro (por empresa o sector)
- Riesgos sistémicos o globales
Para cada riesgo: descripción breve + impacto estimado (Alto/Medio/Bajo).""",

    "kpis": """Extrae todos los indicadores financieros cuantitativos presentes:
- Ratios de valuación (P/E, P/B, EV/EBITDA)
- Rentabilidad (ROE, margen neto)
- Macroeconómicos (tasa, inflación, riesgo país)
- Variaciones de precios (1d, 1m, 12m)
Formato: tabla markdown con columnas: Indicador | Valor | Entidad | Contexto.""",

    "narrativa": """Genera un párrafo de narrativa de mercado de 150-250 palabras, en tono profesional, listo para distribuir a inversores.
Debe incluir: contexto macro, desempeño relativo de los mercados, y perspectiva de corto plazo.
No uses bullets. Texto corrido."""
}


def build_prompt(query: str, context: str, output_type: str) -> tuple[str, str]:
    instr  = OUTPUT_INSTRUCTIONS.get(output_type, OUTPUT_INSTRUCTIONS["resumen"])
    system = f"{SYSTEM_BASE}\n\n{instr}"
    user   = f"CONTEXTO DISPONIBLE:\n{context}\n\n---\nCONSULTA DEL INVERSOR:\n{query}"
    return system, user
