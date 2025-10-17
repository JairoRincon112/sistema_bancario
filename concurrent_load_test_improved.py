#!/usr/bin/env python3
\"\"\"concurrent_load_test_improved.py
Mejoras sobre la versión anterior:
- Parámetros por línea de comandos (host, port, user, password, dbname)
- Retries en conexión con backoff
- Soporte para niveles de aislamiento: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
- Registro detallado por transacción y resumen por corrida (CSV)
- Mejor manejo de excepciones y cierre correcto de conexiones
- Usa ThreadPoolExecutor para control de concurrencia
- Archivo de salida: resultados_concurrencia.csv (detalle) y resumen_concurrencia.csv (agregado)

Uso de ejemplo:
python concurrent_load_test_improved.py --host localhost --port 5432 --user postgres --password secret \
    --dbname banco_concurrencia --concurrency 50 --total 50 --runs 10 --mode 2pl --accounts 1,2,3,4 \
    --isolation 'READ COMMITTED' --out detalle.csv --summary resumen.csv

Nota: debes tener instalado psycopg2-binary:
    pip install psycopg2-binary
\"\"\"

import argparse
import csv
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys

try:
    import psycopg2
    from psycopg2 import OperationalError, DatabaseError
except Exception as e:
    sys.stderr.write(\"ERROR: psycopg2 no está instalado. Instálalo con: pip install psycopg2-binary\\n\") 
    raise

# Helper: create connection with retries
def make_conn(host, port, user, password, dbname, max_retries=3, backoff=1.0):
    attempt = 0
    while True:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
            return conn
        except OperationalError as e:
            attempt += 1
            if attempt > max_retries:
                raise
            time.sleep(backoff * attempt)

# Worker that executes a single transfer using selected mode and isolation level
def worker_task(db_params, mode, isolation_level, from_id, to_id, amt, tx_timeout=30):
    start_ts = datetime.utcnow()
    start = time.time()
    status = None
    err_text = None
    try:
        conn = make_conn(**db_params)
        cur = conn.cursor()
        # Start transaction with requested isolation
        cur.execute(\"SET LOCAL statement_timeout = %s;\", (int(tx_timeout*1000),))
        cur.execute(f\"BEGIN TRANSACTION ISOLATION LEVEL {isolation_level};\")
        try:
            if mode == '2pl':
                cur.execute(\"SELECT banco.transfer_2pl(%s,%s,%s);\", (from_id, to_id, amt))
                status_row = cur.fetchone()
                status = status_row[0] if status_row else 'UNKNOWN'
            elif mode == 'ts':
                cur.execute(\"SELECT banco.transfer_ts(%s,%s,%s, now());\", (from_id, to_id, amt))
                status_row = cur.fetchone()
                status = status_row[0] if status_row else 'UNKNOWN'
            elif mode == 'opt':
                cur.execute(\"SELECT banco.transfer_optimistic(%s,%s,%s);\", (from_id, to_id, amt))
                status_row = cur.fetchone()
                status = status_row[0] if status_row else 'UNKNOWN'
            else:
                # simple manual transfer (not recommended for concurrency experiments)
                cur.execute(\"UPDATE banco.cuenta SET saldo = saldo - %s WHERE cuenta_id = %s RETURNING saldo;\", (amt, from_id))
                cur.execute(\"UPDATE banco.cuenta SET saldo = saldo + %s WHERE cuenta_id = %s RETURNING saldo;\", (amt, to_id))
                cur.execute(\"INSERT INTO banco.transferencia (from_cuenta_id,to_cuenta_id,cantidad,tecnica,estado,nota) VALUES (%s,%s,%s,'SIMPLE','COMPLETADA','manual')\", (from_id, to_id, amt))
                status = 'COMPLETADA'
            conn.commit()
        except Exception as e:
            conn.rollback()
            err_text = str(e).replace('\\n',' | ')
            status = 'ABORT: ' + err_text
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        err_text = str(e).replace('\\n',' | ')
        status = 'CONN_ERR: ' + err_text
    latency = time.time() - start
    end_ts = datetime.utcnow()
    return {
        'start_ts': start_ts.isoformat() + 'Z',
        'end_ts': end_ts.isoformat() + 'Z',
        'mode': mode,
        'isolation': isolation_level,
        'from_id': from_id,
        'to_id': to_id,
        'amount': float(amt),
        'status': status,
        'latency_s': latency,
        'error': err_text
    }

def run_experiment(db_params, concurrency, total_ops, mode, isolation_level, account_pool, amount_range, out_detail, out_summary, run_id):
    results = []
    start_run = time.time()
    # Thread pool
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        for i in range(total_ops):
            from_id = random.choice(account_pool)
            to_id = random.choice([a for a in account_pool if a != from_id])
            amt = round(random.uniform(*amount_range), 2)
            futures.append(ex.submit(worker_task, db_params, mode, isolation_level, from_id, to_id, amt))
        for f in as_completed(futures):
            results.append(f.result())
    end_run = time.time()
    # Write detail CSV (append)
    write_header = False
    try:
        # determine if file exists
        with open(out_detail, 'a', newline='') as df:
            writer = csv.DictWriter(df, fieldnames=list(results[0].keys()))
            if df.tell() == 0:
                writer.writeheader()
            for r in results:
                writer.writerow(r)
    except Exception as e:
        print('No se pudo escribir detalle:', e)
    # Summary metrics
    latencies = [r['latency_s'] for r in results if r and isinstance(r.get('latency_s'), (int,float))]
    statuses = [r['status'] for r in results]
    completed = sum(1 for s in statuses if s and 'COMPLETADA' in s)
    aborted = sum(1 for s in statuses if s and ('ABORT' in s or 'CONN_ERR' in s))
    total = len(results)
    summary = {
        'run_id': run_id,
        'mode': mode,
        'isolation': isolation_level,
        'concurrency': concurrency,
        'total_ops': total,
        'completed': completed,
        'aborted': aborted,
        'abort_rate_pct': round(aborted/total*100.0, 2) if total else 0.0,
        'avg_latency_s': round(statistics.mean(latencies), 4) if latencies else 0.0,
        'p50_latency_s': round(statistics.median(latencies), 4) if latencies else 0.0,
        'throughput_ops_per_s': round(total / (end_run - start_run), 4) if (end_run - start_run) > 0 else 0.0,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }
    # Append summary to CSV
    try:
        with open(out_summary, 'a', newline='') as sf:
            writer = csv.DictWriter(sf, fieldnames=list(summary.keys()))
            if sf.tell() == 0:
                writer.writeheader()
            writer.writerow(summary)
    except Exception as e:
        print('No se pudo escribir resumen:', e)
    return summary

def parse_accounts(s):
    return [int(x.strip()) for x in s.split(',') if x.strip()]

def main():
    parser = argparse.ArgumentParser(description='Concurrent load tester for banco_concurrencia')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--user', default='postgres')
    parser.add_argument('--password', default='')
    parser.add_argument('--dbname', default='banco_concurrencia')
    parser.add_argument('--concurrency', type=int, default=50)
    parser.add_argument('--total', type=int, default=50)
    parser.add_argument('--runs', type=int, default=10)
    parser.add_argument('--mode', choices=['2pl','ts','opt','simple'], default='2pl')
    parser.add_argument('--accounts', default='1,2,3,4')
    parser.add_argument('--amount_min', type=float, default=1.0)
    parser.add_argument('--amount_max', type=float, default=100.0)
    parser.add_argument('--isolation', choices=['READ COMMITTED','REPEATABLE READ','SERIALIZABLE'], default='READ COMMITTED')
    parser.add_argument('--out_detail', default='resultados_concurrencia_detalle.csv')
    parser.add_argument('--out_summary', default='resultados_concurrencia_resumen.csv')
    args = parser.parse_args()

    db_params = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'password': args.password,
        'dbname': args.dbname
    }

    account_pool = parse_accounts(args.accounts)
    amount_range = (args.amount_min, args.amount_max)

    print('Parámetros:', args)

    for run in range(1, args.runs + 1):
        print(f'Iniciando run {run}/{args.runs} mode={args.mode} isolation={args.isolation} concurrency={args.concurrency} total_ops={args.total}')
        summary = run_experiment(db_params, args.concurrency, args.total, args.mode, args.isolation, account_pool, amount_range, args.out_detail, args.out_summary, run)
        print('Resultado resumen:', summary)

if __name__ == '__main__':
    main()
