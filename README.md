# AgroSmart — Monitoramento Agrícola

> Sistema de monitoramento agrícola em tempo real com pipeline de streaming Kafka, containerização Docker e motor de regras booleanas.

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![Flask](https://img.shields.io/badge/Flask-3.x-lightgrey)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.6-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Status](https://img.shields.io/badge/Status-Ativo-brightgreen)

---

## Visão Geral

O **AgroSmart** simula uma rede de sensores distribuídos em talhões de uma propriedade rural. Na Fase 4, a solução evoluiu para um pipeline de streaming completo usando **Apache Kafka**: um produtor publica leituras de sensores continuamente; um consumidor as processa, persiste em CSV e avalia regras de alerta em tempo real; um servidor Flask expõe um dashboard web que se atualiza a cada 3 segundos.

O sistema roda inteiramente em **Docker Compose** — sem instalação manual de dependências.

---

## Arquitetura (Fase 4)

```
Sensores (gerador.py)
    │ JSON a cada 5s
    ▼
producer.py  ──►  Apache Kafka (tópico: sensor_leituras)
                        │
                        ▼
                  main.py (consumer)
                  ├─ Persiste em CSV (thread-safe)
                  ├─ Avalia regras (regras.py)
                  └─ Atualiza alertas em memória
                        │
                        ▼
                  Flask Dashboard (:5000)
                  Kafka UI        (:8080)
```

**Containers:**

| Container | Imagem | Função |
|---|---|---|
| `agro_zookeeper` | `cp-zookeeper:7.6` | Coordenação do cluster Kafka |
| `agro_kafka` | `cp-kafka:7.6` | Broker de mensagens |
| `agro_kafka_ui` | `provectuslabs/kafka-ui` | Interface visual do tópico (porta 8080) |
| `agro_producer` | build local | Publica leituras no Kafka a cada 5s |
| `agro_app` | build local | Dashboard Flask + consumidor Kafka (porta 5000) |

---

## Estrutura do Projeto

```
agro_monitor/
├── main.py              — Flask + consumidor Kafka + modo local (fallback)
├── producer.py          — Produtor Kafka de leituras simuladas
├── gerador.py           — Geração de leituras normais e forçadas
├── regras.py            — Motor de regras: três verificações booleanas
├── Dockerfile           — Imagem Python 3.11 slim
├── docker-compose.yml   — Stack completa: Kafka + UI + producer + app
├── requirements.txt     — flask, pandas, kafka-python
├── dados/
│   └── leituras.csv     — Histórico de leituras (criado automaticamente)
└── docs/
    ├── pipeline_streaming.html  — Diagrama do pipeline de dados
    ├── canvas_negocios.html     — Business Model Canvas
    └── fluxograma_decisao.html  — Fluxograma do motor de regras
```

---

## Execução com Docker (recomendado)

```bash
# Clonar / entrar na pasta
cd agro_monitor

# Subir toda a stack
docker compose up --build

# Acessar:
# Dashboard:  http://localhost:5000
# Kafka UI:   http://localhost:8080
```

Para parar:

```bash
docker compose down
```

---

## Execução local (sem Docker)

```bash
pip install flask pandas kafka-python

# Modo local (sem Kafka — igual à Fase 3)
python main.py

# Abrir: http://localhost:5000
```

O `main.py` detecta automaticamente se o Kafka está disponível. Se não estiver, usa a thread local de geração — sem nenhuma configuração extra.

---

## Pipeline de Dados com Streaming

Diagrama completo: [`docs/pipeline_streaming.html`](docs/pipeline_streaming.html)

**Fluxo:**

1. `producer.py` gera leituras JSON a cada 5 segundos e publica no tópico `sensor_leituras`
2. O Kafka armazena e distribui as mensagens (chave = `talhao_id`)
3. `main.py` em modo `consumer` consome o tópico, persiste cada leitura no CSV e avalia as regras
4. As regras booleanas detectam anomalias e atualizam a lista de alertas em memória
5. O frontend faz polling `/api/dados` a cada 3s e renderiza alertas e leituras

---

## Regras de Decisão

| Regra | Condição Lógica | Nível | Ação |
|---|---|---|---|
| INFESTAÇÃO | `perc_folhas_doentes > 30` **AND** `praga_detectada != "nenhuma"` | CRÍTICO | Alerta imediato; talhão marcado para monitoramento intensivo |
| IRRIGAÇÃO  | `umidade_solo_pct < 30` **AND** `nivel_irrigacao == "baixo"` | ATENÇÃO | Solicitação de irrigação emergencial |
| TEMPERATURA | `temperatura_c > 35` **OR** `temperatura_c < 12` | AVISO | Registro de anomalia climática |

---

## Modelo de Negócio

Canvas completo: [`docs/canvas_negocios.html`](docs/canvas_negocios.html)

**Resumo da Proposta de Valor:**

- Alertas em tempo real de infestação, irrigação e temperatura
- Redução estimada de 15–30% nas perdas de colheita
- SaaS: Starter R$ 299/mês · Pro R$ 799/mês · Enterprise sob consulta
- Segmentos: médios e grandes produtores, cooperativas, integradores

---

## Instalação e Dependências

```
flask>=3.0.0
pandas>=2.0.0
kafka-python>=2.0.2
```

---

## Contexto Acadêmico

Projeto entregue como atividade da **Fase 4** de automação agrícola — FIAP · Turma 4ESOA.

### Requisitos atendidos

1. **Pipeline de Dados com Streaming** — Apache Kafka com producer/consumer, `docker-compose.yml`, diagrama em `docs/pipeline_streaming.html`
2. **Containerização** — `Dockerfile` + `docker-compose.yml` com 5 serviços
3. **Modelo de Negócio com Canvas** — `docs/canvas_negocios.html`

### Time

| Nome | RM | Turma |
|---|---|---|
| Gabriel Genaro Dalaqua | 551986 | 4ESOA |
| Alairton Rocha Scabelli | 551454 | 4ESOA |
| Carolina Nascimento Amorim | 97930 | 4ESOA |
| Eduardo Marins | 551892 | 4ESOA |
| Sarah Ribeiro da Silva | 97747 | 4ESOA |
