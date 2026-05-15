"""
gerador.py — Geração de leituras simuladas de sensores agrícolas.

Fornece leituras normais (distribuição realista) e leituras forçadas
(garantem o disparo de uma regra específica), além de persistência
thread-safe em CSV.
"""

import csv
import os
import random
import threading
from datetime import datetime

# Lock global compartilhado com main.py (injetado via set_lock)
_csv_lock = threading.Lock()


def set_lock(lock: threading.Lock) -> None:
    """Substitui o lock interno pelo lock central criado em main.py."""
    global _csv_lock
    _csv_lock = lock


def _campos() -> list[str]:
    return [
        "timestamp",
        "talhao_id",
        "perc_folhas_doentes",
        "praga_detectada",
        "umidade_solo_pct",
        "temperatura_c",
        "nivel_irrigacao",
    ]


def _base_normal() -> dict:
    """Valores-padrão dentro de faixas realistas, usados como base pelas duas funções."""
    pragas = ["pulgão", "lagarta", "ácaro", "nenhuma"]
    praga = random.choices(pragas, weights=[10, 10, 10, 70])[0]

    irrigacao = random.choices(["baixo", "normal", "alto"], weights=[20, 60, 20])[0]

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "talhao_id": random.randint(1, 8),
        "perc_folhas_doentes": round(random.uniform(5.0, 25.0), 2),
        "praga_detectada": praga,
        "umidade_solo_pct": round(random.uniform(40.0, 80.0), 2),
        "temperatura_c": round(random.uniform(18.0, 32.0), 2),
        "nivel_irrigacao": irrigacao,
    }


def gerar_leitura_normal() -> dict:
    """
    Gera uma leitura com valores aleatórios dentro de faixas realistas.
    Probabilidades: 70% sem praga; 60% irrigação normal; temperatura 18–32 °C.
    """
    return _base_normal()


def gerar_leitura_forcada(tipo_alerta: str) -> dict:
    """
    Gera uma leitura com valores que GARANTEM o disparo da regra indicada.

    tipo_alerta:
      "infestacao"  → perc_folhas_doentes > 30 AND praga_detectada != "nenhuma"
      "irrigacao"   → umidade_solo_pct < 30 AND nivel_irrigacao == "baixo"
      "temperatura" → temperatura_c > 35 OR temperatura_c < 12
    """
    leitura = _base_normal()

    if tipo_alerta == "infestacao":
        leitura["perc_folhas_doentes"] = round(random.uniform(40.0, 80.0), 2)
        leitura["praga_detectada"] = random.choice(["pulgão", "lagarta", "ácaro"])

    elif tipo_alerta == "irrigacao":
        leitura["umidade_solo_pct"] = round(random.uniform(5.0, 25.0), 2)
        leitura["nivel_irrigacao"] = "baixo"

    elif tipo_alerta == "temperatura":
        # Sorteia entre calor extremo (>35) ou frio extremo (<12)
        if random.random() < 0.5:
            leitura["temperatura_c"] = round(random.uniform(36.0, 42.0), 2)
        else:
            leitura["temperatura_c"] = round(random.uniform(8.0, 11.0), 2)

    return leitura


def salvar_no_csv(leitura: dict, caminho: str = "dados/leituras.csv") -> None:
    """
    Faz APPEND da leitura no CSV.
    Cria o arquivo com cabeçalho se ele ainda não existir.
    Thread-safe: usa _csv_lock para evitar escrita concorrente.
    """
    os.makedirs(os.path.dirname(caminho) if os.path.dirname(caminho) else ".", exist_ok=True)
    campos = _campos()

    with _csv_lock:
        arquivo_novo = not os.path.exists(caminho)
        with open(caminho, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            if arquivo_novo:
                writer.writeheader()
            writer.writerow({k: leitura[k] for k in campos})
