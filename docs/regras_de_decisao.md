# Regras de Decisão — Sistema de Monitoramento Agrícola

## 1. Visão Geral

O sistema utiliza um motor de regras baseado em lógica booleana para interpretar continuamente as leituras geradas pelos sensores agrícolas distribuídos nos talhões da propriedade. A cada ciclo de 5 segundos, o módulo `regras.py` avalia as últimas 50 linhas registradas no arquivo CSV e aplica três regras de decisão independentes. Quando uma ou mais regras são satisfeitas, o sistema produz alertas classificados por nível de severidade e os exibe no dashboard, disparando ações automatizadas sem necessidade de intervenção humana.

---

## 2. Variáveis de Entrada

As seguintes variáveis são lidas do arquivo `dados/leituras.csv` e alimentam diretamente as condições das regras:

| Variável | Tipo | Faixa de Valores | Descrição |
|---|---|---|---|
| `timestamp` | string (ISO 8601) | data/hora da leitura | Momento em que o sensor registrou os dados |
| `talhao_id` | inteiro | 1 – 8 | Identificador do talhão monitorado |
| `perc_folhas_doentes` | float | 5.0 – 25.0 (normal) / até 80.0 (forçado) | Percentual de folhas com sintomas de doença |
| `praga_detectada` | string | `nenhuma`, `pulgão`, `lagarta`, `ácaro` | Tipo de praga identificada pelo sensor |
| `umidade_solo_pct` | float | 40.0 – 80.0 (normal) / até 5.0 (forçado) | Umidade volumétrica do solo em percentual |
| `temperatura_c` | float | 18.0 – 32.0 (normal) / até 42.0 ou 8.0 (forçado) | Temperatura do ar em graus Celsius |
| `nivel_irrigacao` | string | `baixo`, `normal`, `alto` | Nível de operação atual do sistema de irrigação |

---

## 3. Regras de Decisão

### Regra 1 — INFESTAÇÃO

**Condição lógica (notação booleana):**

```
SE (perc_folhas_doentes > 30) AND (praga_detectada != "nenhuma")
ENTÃO disparar alerta de nível CRÍTICO
```

**Pseudocódigo:**

```
SE perc_folhas_doentes > 30 AND praga_detectada != "nenhuma":
    alerta.tipo    ← "INFESTAÇÃO"
    alerta.nivel   ← "CRÍTICO"
    alerta.detalhe ← "<X>% de folhas doentes — praga: <tipo>"
    REGISTRAR talhão como Monitoramento Intensivo
    NOTIFICAR gestor da fazenda via sistema
    ELEVAR frequência de leitura dos sensores para 60 s no talhão
    GERAR registro de ocorrência de praga no histórico
```

**Tabela de verdade (operador AND):**

| perc_folhas_doentes > 30 | praga_detectada != "nenhuma" | Resultado |
|---|---|---|
| Verdadeiro | Verdadeiro | **ALERTA CRÍTICO** |
| Verdadeiro | Falso | Sem alerta |
| Falso | Verdadeiro | Sem alerta |
| Falso | Falso | Sem alerta |

**Descrição em linguagem natural:**

Se mais de 30% das folhas do talhão apresentarem sintomas de doença **e** uma praga for detectada simultaneamente, o sistema dispara um alerta crítico exigindo aplicação de defensivo agrícola e inspeção imediata de campo.

**Nível de severidade e ação disparada:**

Nível **CRÍTICO** — exibido como card vermelho no dashboard. O sistema registra a ocorrência no histórico, marca o talhão para monitoramento intensivo, eleva a frequência de leitura e notifica o gestor da fazenda.

---

### Regra 2 — IRRIGAÇÃO

**Condição lógica (notação booleana):**

```
SE (umidade_solo_pct < 30) AND (nivel_irrigacao == "baixo")
ENTÃO disparar alerta de nível ATENÇÃO
```

**Pseudocódigo:**

```
SE umidade_solo_pct < 30 AND nivel_irrigacao == "baixo":
    alerta.tipo    ← "IRRIGAÇÃO"
    alerta.nivel   ← "ATENÇÃO"
    alerta.detalhe ← "Umidade em <X>% com irrigação baixa"
    ENVIAR solicitação de irrigação emergencial ao controlador de campo
    REGISTRAR alerta de solo seco no histórico do talhão
    NOTIFICAR equipe de manutenção sobre possível falha no sistema
    ELEVAR prioridade do talhão no índice de monitoramento
```

**Tabela de verdade (operador AND):**

| umidade_solo_pct < 30 | nivel_irrigacao == "baixo" | Resultado |
|---|---|---|
| Verdadeiro | Verdadeiro | **ALERTA ATENÇÃO** |
| Verdadeiro | Falso | Sem alerta |
| Falso | Verdadeiro | Sem alerta |
| Falso | Falso | Sem alerta |

**Descrição em linguagem natural:**

Se o solo estiver seco (umidade abaixo de 30%) **e** o sistema de irrigação já estiver operando em nível baixo, o sistema identifica risco de déficit hídrico e solicita irrigação emergencial ao controlador de campo.

**Nível de severidade e ação disparada:**

Nível **ATENÇÃO** — exibido como card amarelo no dashboard. O sistema envia solicitação de irrigação emergencial, registra o alerta no histórico do talhão e notifica a equipe de manutenção sobre possível falha no sistema de irrigação.

---

### Regra 3 — TEMPERATURA

**Condição lógica (notação booleana):**

```
SE (temperatura_c > 35) OR (temperatura_c < 12)
ENTÃO disparar alerta de nível AVISO
```

**Pseudocódigo:**

```
SE temperatura_c > 35 OR temperatura_c < 12:
    alerta.tipo    ← "TEMPERATURA"
    alerta.nivel   ← "AVISO"
    alerta.detalhe ← "Temperatura de <X>°C fora da faixa segura"
    SE temperatura_c > 35:
        recomendacoes ← [irrigação noturna, sombreamento, risco de queima foliar]
    SENÃO:  // temperatura_c < 12
        recomendacoes ← [cobertura protetora, antecipação de colheita, risco de geada]
    REGISTRAR anomalia climática no log de eventos do sistema
    GERAR relatório de anomalia de temperatura automaticamente
    NOTIFICAR responsável técnico da área
    ELEVAR frequência de monitoramento do talhão por 30 min
```

**Tabela de verdade (operador OR):**

| temperatura_c > 35 | temperatura_c < 12 | Resultado |
|---|---|---|
| Verdadeiro | Falso | **ALERTA AVISO** (calor extremo) |
| Falso | Verdadeiro | **ALERTA AVISO** (frio extremo) |
| Falso | Falso | Sem alerta |
| Verdadeiro | Verdadeiro | Impossível fisicamente |

**Descrição em linguagem natural:**

Basta **uma** das condições ser verdadeira: se a temperatura estiver acima de 35°C (risco de queima foliar) **ou** abaixo de 12°C (risco de geada), o sistema emite um aviso com recomendações específicas para o tipo de anomalia detectada.

**Nível de severidade e ação disparada:**

Nível **AVISO** — exibido como card azul no dashboard. O sistema registra a anomalia no log de eventos, gera relatório automático, notifica o responsável técnico e aumenta temporariamente a frequência de monitoramento do talhão.

---

## 4. Fluxo Geral de Avaliação

Pipeline completo executado pela thread daemon (`loop_geracao` em `main.py`) a cada 5 segundos:

```
LOOP a cada 5 segundos:
    leitura  ← GERAR dados do sensor    // gerador.gerar_leitura_normal()
    SALVAR leitura no CSV               // append thread-safe com threading.Lock
    df       ← LER últimas 50 linhas do CSV
    alertas  ← AVALIAR todas as regras sobre df:
                 alertas += verificar_infestacao(df)   // operador AND
                 alertas += verificar_irrigacao(df)    // operador AND
                 alertas += verificar_temperatura(df)  // operador OR
                 ORDENAR alertas por timestamp DESC
    ATUALIZAR alertas_recentes em memória  // lista compartilhada, máx 50 itens
    AGUARDAR 5 segundos

// Em paralelo, a cada 3 segundos o dashboard faz:
    GET /api/dados → lê alertas_recentes + últimas 20 linhas do CSV → atualiza UI
```

---

## 5. Níveis de Severidade

| Nível | Cor no Dashboard | Regra Associada | Ação Recomendada |
|---|---|---|---|
| **CRÍTICO** | Vermelho (`#ef4444`) | Infestação | Aplicar defensivo agrícola, isolar o talhão e acionar equipe de campo imediatamente |
| **ATENÇÃO** | Amarelo (`#f59e0b`) | Irrigação | Acionar irrigação suplementar e verificar linhas de gotejamento e sensores de umidade |
| **AVISO** | Azul (`#3b82f6`) | Temperatura | Avaliar irrigação noturna ou sombreamento (calor) / cobertura protetora contra geada (frio) |
