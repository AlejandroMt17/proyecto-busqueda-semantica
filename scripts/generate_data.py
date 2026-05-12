#!/usr/bin/env python3
"""
Generación reproducible de dataset para Semana 1 y carga a MinIO (S3 API).

Cumple mínimos típicos del enunciado:
  - Texto: >= 500_000 documentos (JSONL, un objeto JSON por línea).
  - Tabular: >= 10_000_000 filas en CSV (alternativa al criterio de 5 GB CSV).

Todo el muestreo es determinista vía numpy.random.Generator(seed).

Uso (MinIO ya arriba, ver README):
  pip install -r requirements.txt
  python scripts/generate_data.py --seed 42

Prueba rápida (pocas filas, sin subir):
  python scripts/generate_data.py --dry-run
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from pathlib import Path

import boto3
import numpy as np
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from tqdm import tqdm

# Semilla fija por defecto (reproducibilidad académica).
DEFAULT_SEED = 42
DEFAULT_DOCS = 500_000
DEFAULT_TABULAR_ROWS = 10_000_000

# Léxico fijo (determinista): solo se indexa por posición con el RNG.
WORDS_ES = (
    "sistema motor documento texto búsqueda semántica vector embedding spark "
    "cluster datos pipeline etl inferencia elastic minio almacenamiento "
    "documentos párrafo frase token modelo entrenamiento evaluación métrica "
    "latencia throughput partición shuffle executor driver worker master "
    "red hdfs s3 objeto bucket csv jsonl wiki artículo sección resumen "
    "título contenido metadato identificador versión fecha lote semilla "
    "reproducible generación sintética volumen requisito entrega semana "
    "equipo integración arquitectura diagrama diseño decisión esquema plan "
    "fase feature predicción índice consulta similitud coseno distancia "
    "normalización limpieza chunk ventana solapamiento codificación utf8 "
    "python java scala hadoop yarn kubernetes contenedor orquestación "
    "monitorización log traza error éxito validación muestreo estratificado "
    "tabular fila columna etiqueta categoría numérico entero flotante ruido "
    "distribución uniforme binomial poisson gaussiana correlación sesgo "
    "outlier imputación agregación suma media mediana desviación percentil "
    "histograma bucketización particionado compresión gzip parquet delta "
    "lakehouse gobernanza calidad linaje auditoría seguridad acceso rol "
    "política retención backup recuperación desastre coste optimización "
    "benchmark prueba carga estrés caos resiliencia disponibilidad "
    "consistencia partición global local incremental batch streaming "
    "checkpoint watermark latencia fin ventana tumbling hopping session "
    "join agregado estado materialización vista incremental deduplicación "
    "idempotencia transacción aislamiento durabilidad atomicidad "
    "consistencia eventual fuerte serializable lectura escritura bloqueo "
    "deadlock contención cola prioridad backpressure presión memoria cpu "
    "gpu aceleración cuantización compresión cuantificada destilación "
    "fine tuning pretraining corpus dataset benchmark leaderboard baseline "
    "ablation estudio comparativa métrica f1 precisión recall exactitud "
    "calibración umbral ranking ndcg map precision at k recall at k "
    "embedding contextualizado subword bytepair tokenización normalización "
    "unicode ascii diacrítico lematización stemming stopwords ngram bigram "
    "trigram tfidf bm25 lexical densidad entropía perplexidad suavizado "
    "laplace dirichlet bayes naive regresión logística svm bosque aleatorio "
    "gradiente boosting árbol profundidad hoja poda regularización l1 l2 "
    "elasticnet dropout early stopping validación cruzada kfold estratificado "
    "bootstrap jackknife intervalo confianza significancia pvalor contraste "
    "hipótesis nula alternativa potencia tamaño muestral efecto práctico "
    "visualización dispersión serie temporal estacionalidad tendencia "
    "anomalía detección segmentación agrupamiento kmeans jerárquico dbscan "
    "densidad silueta davies bouldin índice calinski harabasz "
    "reducción dimensionalidad pca svd tsne umap autoencoder variacional "
    "generativo adversario difusión ruido paso scheduler cosine schedule "
    "warmup decaimiento peso gradiente clip norma acumulador escala mixta "
    "precisión entrenamiento inferencia batch tamaño época iteración paso "
    "learning rate momentum nesterov adam adamw rmsprop adagrad adadelta "
    "segundo momento estimación sesgada corrección bias weight decay "
    "regularización espectral normalización capa batchnorm groupnorm "
    "instancenorm layernorm activación relu gelu swish mish tanh sigmoid "
    "softmax logsoftmax focal loss contraste triplet center sphereface "
    "arcfac cosface amsoftmax prototipo métrica aprendida kernel rbf "
    "polinomial sigmoid string similitud coseno producto punto manhattan "
    "chebyshev minkowski haversine geoespacial índice hnsw ivf pq opq "
    "quantización producto búsqueda aproximada vecino más cercano exacto "
    "rango filtro metadata facetado facet agregación compuesta "
    "orquestador airflow dag tarea dependencia sensor operador hook "
    "plantilla macro variable entorno secreto conexión pool límites "
    "reintentos idempotencia compensación saga coreografía orquestación "
    "microservicio monolito modular desacoplamiento contrato esquema "
    "versionado compatibilidad migración expansión contracción "
    "particionamiento sharding replicación factor consistencia quorum "
    "elección líder consenso raft paxos zab gossip membership failure "
    "detector heartbeat suscripción publicación cola mensaje broker "
    "kafka pulsar amqp mqtt websocket grpc rest openapi swagger "
    "autenticación autorización oauth oidc jwt jwks tls mTLS pinning "
    "certificado cadena revocación ocsp stapling hsts csp cors csrf "
    "sanitización escape inyección validación esquema jsonschema avro "
    "protobuf thrift parquet orc feather zstd lz4 snappy brotli gzip "
    "checksum crc32c md5 sha256 merkle bloom filtro probabilístico "
    "conteo aproximado cardinalidad hyperloglog minhash simhash "
    "shingle ngram similitud jaccard dice overlap coeficiente sorensen "
    "distancia edición levenshtein damerau jaro winkler "
    "normalización unicode nfkc nfc nfd nfkd compatibilidad collation "
    "orden lexicográfico binario locale cultura zona horaria utc offset "
    "dst verano invierno calendario juliano gregoriano iso8601 rfc3339 "
    "epoch milisegundo microsegundo nanosegundo reloj monotónico wall "
    "sincronización ntp ptp cronométrico jitter deriva skew drift "
    "alineación muestreo downsampling upsampling interpolación spline "
    "suavizado exponencial holt winters arima sarima prophet kalman "
    "filtro partícula bayesiano variacional inferencia aproximada mcmc "
    "hamiltoniano langevin nuts hmc vi elbo reconstrucción divergencia "
    "kl js wasserstein frechet inception score fid clip score "
    "alineamiento atención cabeza multiquery groupedquery flash "
    "kernel fusion tiling warp shuffle bank conflicto memoria compartida "
    "caché línea asociativa directa multilevel inclusión exclusión "
    "política reemplazo lru lfu mru random clock segunda oportunidad "
    "prefetch readahead writeback writethrough journaling copyonwrite "
    "snapshot clone deduplicación thin thick sparse provisioning "
    "cuota límite throttling rate burst token leaky bucket cola "
    "prioridad fair weighted roundrobin leastconn iphash consistent "
    "hash ring virtual vnode rendezvous jump shuffle sharding rebalance "
    "migration handoff split merge compaction minor major full freeze "
    "flush checkpoint wal redo undo vacuum analyze vacuum full "
    "autovacuum bloat fragmentation fillfactor padding alignment "
    "endianness little big serialización marshalling pickling avro "
    "schema registry compatibilidad backward forward full transitive "
    "evolución contrato consumidor productor idempotency key dedupe "
    "exactly once at least once at most once orden total causal "
    "vector reloj lamport chandy lamport snapshot chandy misra "
    "terminación detección deadlock recursos grafo espera ciclo "
    "prevención evitación detección recuperación rollback compensación "
    "saga orquestación coreografía patrón outbox inbox transactional "
    "message table event sourcing cqrs proyección materializada "
    "readmodel writemodel consistencia fuerte eventual sesión "
    "sticky routing affinity antiaffinity toleration taint nodo "
    "pod deployment statefulset daemonset job cron hpa vpa keda "
    "escalado horizontal vertical burst spot preemptible reservado "
    "dedicado compartido multitenancy aislamiento noisy vecino "
    "burst cpu memoria io red disco ssd nvme hdd sas sata scsi "
    "iscsi fc nvmeof rdma roce infiniband ethernet jumbo frame mtu "
    "segmentación vlan vxlan geneve gre ipsec wireguard tailscale "
    "zerotrust bastión jumpbox sso mfa phishing resistant webauthn "
    "u2f fido passkey device bound attestación remota plataforma "
    "confianza medición integridad arranque seguro cadena confianza "
    "medición arranque tpm secure boot measured boot attestation "
    "remote attestation quote pcr bank algorithm sha1 sha256 sm3 "
    "postcuántico lattice hash multivariate code isogeny supersingular "
    "curva elíptica ed25519 x25519 secp256r1 brainpool random "
    "nonce salt pepper stretching pbkdf2 scrypt argon2 bcrypt "
    "memoria hardening aslr dep canary stack guard safe stack "
    "shadow stack cfi control flow integrity return oriented "
    "programming mitigation sandboxing seccomp apparmor selinux "
    "capabilities principle least privilege separation duties "
    "dual control maker checker four eyes breakglass emergency "
    "access audited session recording immutable log append only "
    "hash chain merkle tree transparency certificate transparency "
    "revocation stapling ocsp must staple expect ct dnssec dane "
    "tlsa smimea openpgpkey sshfp tlsa https br skiptls "
    "encrypted client hello ech grease extension padding record "
    "size limit fragmentation reassembly congestion control cubic "
    "bbr reno vegas yeah illinois scalable tcp fast open sack "
    "timestamp window scaling selective acknowledgement duplicate "
    "sack dsack ecn explicit congestion notification l4s dualq "
    "coupled aqm scheduling fq codel cake pie htb hfsc cbq "
    "prio sfq esfq ingress egress policing shaping marking "
    "classification filter match action redirect mirror tee "
    "sample police drop continue goto chain hook netfilter "
    "iptables nftables ebpf xdp tc clsact ingress egress "
    "bpf map ringbuf perf buffer tracepoint kprobe uprobe "
    "uprobe syscall trace ftrace perf sched latency histogram "
    "flamegraph offcpu oncpu lock wait block io scheduler mq "
    "deadline noop bfq kyber"
).split()


def build_s3_client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def synthetic_body(rng: np.random.Generator, doc_id: int) -> str:
    n_words = int(rng.integers(80, 420))
    idx = rng.integers(0, len(WORDS_ES), size=n_words)
    words = [WORDS_ES[i] for i in idx]
    return f"doc_id={doc_id} " + " ".join(words)


def write_docs_shard(
    path: Path,
    rng: np.random.Generator,
    seed: int,
    start_id: int,
    count: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as gz:
        for i in tqdm(range(count), desc=f"docs {start_id}..{start_id + count - 1}", leave=False):
            doc_id = start_id + i
            record = {
                "id": doc_id,
                "title": f"Documento sintético seed={seed} id={doc_id}",
                "text": synthetic_body(rng, doc_id),
            }
            gz.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_tabular_csv(path: Path, rng: np.random.Generator, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    import csv

    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "x1", "x2", "x3", "label"])
        for i in tqdm(range(rows), desc="tabular csv"):
            w.writerow(
                [
                    i,
                    f"{rng.random():.8f}",
                    f"{rng.random():.8f}",
                    f"{rng.random():.8f}",
                    int(rng.integers(0, 5)),
                ]
            )


def upload_file(client, bucket: str, key: str, local_path: Path) -> None:
    client.upload_file(str(local_path), bucket, key)


def ensure_bucket(client, name: str) -> None:
    """Crea el bucket si no existe (útil sin docker-compose / minio-init)."""
    try:
        client.head_bucket(Bucket=name)
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code not in ("404", "403", "NoSuchBucket"):
            raise
    try:
        client.create_bucket(Bucket=name)
    except ClientError as e2:
        c2 = e2.response.get("Error", {}).get("Code", "")
        if c2 in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            return
        raise


def check_endpoint(client, endpoint: str) -> None:
    try:
        client.list_buckets()
    except EndpointConnectionError as e:
        msg = (
            f"No hay ningún servidor S3 escuchando en {endpoint}.\n"
            "Opciones en Windows:\n"
            "  1) Instalar Docker Desktop, reiniciar la sesión y ejecutar: docker compose up -d\n"
            "  2) Sin Docker: en otra consola PowerShell ejecutar:\n"
            "       .\\scripts\\start_minio_windows.ps1\n"
            "     (dejá esa ventana abierta; API en http://127.0.0.1:9000 )\n"
        )
        raise SystemExit(msg) from e


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Dataset reproducible + MinIO (S3).")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Semilla fija (numpy).")
    p.add_argument("--docs", type=int, default=DEFAULT_DOCS, help="Cantidad de documentos JSONL.")
    p.add_argument(
        "--tabular-rows",
        type=int,
        default=DEFAULT_TABULAR_ROWS,
        help="Filas del CSV tabular.",
    )
    p.add_argument("--docs-per-shard", type=int, default=100_000, help="Líneas por archivo JSONL.gz.")
    p.add_argument(
        "--endpoint",
        default=os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000"),
        help="URL S3 (MinIO).",
    )
    p.add_argument("--access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    p.add_argument("--secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"))
    p.add_argument("--bucket-docs", default=os.environ.get("MINIO_BUCKET_DOCS", "semantic-raw"))
    p.add_argument(
        "--bucket-tabular",
        default=os.environ.get("MINIO_BUCKET_TABULAR", "semantic-tabular"),
    )
    p.add_argument(
        "--prefix-docs",
        default="text/v1",
        help="Prefijo dentro del bucket de documentos.",
    )
    p.add_argument(
        "--prefix-tabular",
        default="tabular/v1",
        help="Prefijo dentro del bucket tabular.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Genera pocos registros en ./data/generated_smoke y no sube a MinIO.",
    )
    p.add_argument(
        "--skip-tabular",
        action="store_true",
        help="No genera ni sube el CSV grande.",
    )
    p.add_argument(
        "--skip-docs",
        action="store_true",
        help="No genera ni sube documentos.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    if args.dry_run:
        args.docs = 2_000
        args.tabular_rows = 20_000
        print("[dry-run] docs=2000 tabular_rows=20000 (sin subida a MinIO)")

    rng = np.random.default_rng(args.seed)

    root = Path(__file__).resolve().parents[1]
    local_out = root / "data" / ("generated_smoke" if args.dry_run else "generated_upload_tmp")
    local_out.mkdir(parents=True, exist_ok=True)

    client = None
    if not args.dry_run:
        client = build_s3_client(args.endpoint, args.access_key, args.secret_key)
        check_endpoint(client, args.endpoint)
        ensure_bucket(client, args.bucket_docs)
        ensure_bucket(client, args.bucket_tabular)

    if not args.skip_docs:
        if args.docs < 500_000 and not args.dry_run:
            print("Advertencia: --docs < 500000 no cumple el mínimo textual del enunciado.", file=sys.stderr)

        shard = args.docs_per_shard
        start = 0
        shard_idx = 0
        while start < args.docs:
            n = min(shard, args.docs - start)
            tmp = local_out / f"docs_part_{shard_idx:05d}.jsonl.gz"
            write_docs_shard(tmp, rng, args.seed, start, n)
            key = f"{args.prefix_docs}/docs_part_{shard_idx:05d}.jsonl.gz"
            if client is not None:
                upload_file(client, args.bucket_docs, key, tmp)
                print(f"Subido s3://{args.bucket_docs}/{key} ({tmp.stat().st_size} bytes)")
                tmp.unlink(missing_ok=True)
            else:
                print(f"[dry-run] escrito {tmp} ({tmp.stat().st_size} bytes)")
            start += n
            shard_idx += 1

    if not args.skip_tabular:
        if args.tabular_rows < 10_000_000 and not args.dry_run:
            print(
                "Advertencia: --tabular-rows < 10000000 no cumple el mínimo tabular del enunciado.",
                file=sys.stderr,
            )

        tab_path = local_out / "events.csv"
        write_tabular_csv(tab_path, rng, args.tabular_rows)
        if client is not None:
            key = f"{args.prefix_tabular}/events_seed{args.seed}.csv"
            upload_file(client, args.bucket_tabular, key, tab_path)
            print(f"Subido s3://{args.bucket_tabular}/{key} ({tab_path.stat().st_size} bytes)")
            tab_path.unlink(missing_ok=True)
        else:
            print(f"[dry-run] escrito {tab_path} ({tab_path.stat().st_size} bytes)")

    # Limpieza: solo borrar carpeta temporal de subidas si quedó vacía
    if not args.dry_run:
        try:
            next(local_out.iterdir())
        except StopIteration:
            local_out.rmdir()

    print("Listo. Semilla:", args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
