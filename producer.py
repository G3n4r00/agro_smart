"""
producer.py — Produtor Kafka para leituras simuladas de sensores agrícolas.

Publica mensagens JSON no tópico 'sensor_leituras' continuamente.
Roda como serviço independente dentro do docker-compose.

Variáveis de ambiente:
    KAFKA_BOOTSTRAP  — endereço do broker (padrão: localhost:9092)
    KAFKA_TOPIC      — nome do tópico    (padrão: sensor_leituras)
    INTERVALO_SEG    — intervalo entre mensagens em segundos (padrão: 5)
"""

import json
import os
import time

import random

from kafka import KafkaProducer

from gerador import gerar_leitura_normal, gerar_leitura_forcada

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC     = os.getenv("KAFKA_TOPIC", "sensor_leituras")
INTERVALO = float(os.getenv("INTERVALO_SEG", "5"))


def criar_producer(retries: int = 20, delay: int = 5) -> KafkaProducer:
    for tentativa in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
            )
            print(f"[PRODUCER] Conectado ao Kafka em {BOOTSTRAP}")
            return p
        except Exception:
            print(f"[PRODUCER] Broker indisponível — tentativa {tentativa}/{retries}. Aguardando {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"[PRODUCER] Falha ao conectar ao Kafka após {retries} tentativas.")


def main():
    producer = criar_producer()
    tipos_forcados = ["infestacao", "irrigacao", "temperatura"]
    ciclo = 0

    print(f"[PRODUCER] Publicando em tópico '{TOPIC}' a cada {INTERVALO}s")

    while True:
        ciclo += 1

        # ~15% das mensagens são leituras forçadas (simulam eventos anômalos)
        if random.random() < 0.15:
            tipo = random.choice(tipos_forcados)
            leitura = gerar_leitura_forcada(tipo)
            leitura["_forcada"] = tipo
        else:
            leitura = gerar_leitura_normal()

        producer.send(
            TOPIC,
            key=str(leitura["talhao_id"]),
            value=leitura,
        )
        producer.flush()

        print(
            f"[PRODUCER #{ciclo}] talhão={leitura['talhao_id']} "
            f"temp={leitura['temperatura_c']:.1f}°C "
            f"umid={leitura['umidade_solo_pct']:.1f}% "
            f"praga={leitura['praga_detectada']}"
        )

        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
