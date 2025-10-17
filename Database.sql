-- CONTROL DE CONCURRENCIA - SCRIPT FINAL 

-- =====================
-- SCHEMA + TABLAS
-- =====================
CREATE SCHEMA IF NOT EXISTS banco AUTHORIZATION CURRENT_USER;
SET search_path TO banco, public;

CREATE TABLE IF NOT EXISTS banco.cuenta (
    cuenta_id BIGSERIAL PRIMARY KEY,
    titular VARCHAR(200) NOT NULL,
    saldo NUMERIC(18,2) NOT NULL CHECK (saldo >= 0),
    moneda VARCHAR(3) NOT NULL DEFAULT 'COP',
    version BIGINT NOT NULL DEFAULT 1,
    fecha_creacion TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS banco.transferencia (
    transferencia_id BIGSERIAL PRIMARY KEY,
    from_cuenta_id BIGINT NOT NULL REFERENCES banco.cuenta(cuenta_id) ON DELETE RESTRICT,
    to_cuenta_id BIGINT NOT NULL REFERENCES banco.cuenta(cuenta_id) ON DELETE RESTRICT,
    cantidad NUMERIC(18,2) NOT NULL CHECK (cantidad > 0),
    tecnica VARCHAR(20) NOT NULL,
    estado VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    nota TEXT
);

CREATE TABLE IF NOT EXISTS banco.lock_manager (
    lm_id BIGSERIAL PRIMARY KEY,
    recurso TEXT NOT NULL,
    recurso_hash BIGINT NOT NULL,
    session_pid INTEGER NOT NULL,
    tipo_lock VARCHAR(10) NOT NULL,
    estado VARCHAR(20) NOT NULL DEFAULT 'HELD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS banco.timestamp_manager (
    ts_id BIGSERIAL PRIMARY KEY,
    recurso TEXT NOT NULL UNIQUE,
    last_read_ts TIMESTAMPTZ,
    last_write_ts TIMESTAMPTZ
);

-- Datos iniciales (idempotente)
INSERT INTO banco.cuenta (titular, saldo)
VALUES
('Alice', 10000.00),
('Bob',   8000.00),
('Carol', 5000.00),
('David', 3000.00)
ON CONFLICT DO NOTHING;

-- =====================
-- LIMPIEZA DE FUNCIONES PREVIAS (para evitar errores)
-- =====================
DROP FUNCTION IF EXISTS banco.recurso_hash_int(TEXT);
DROP FUNCTION IF EXISTS banco.request_lock(TEXT, VARCHAR);
DROP FUNCTION IF EXISTS banco.release_lock(TEXT);
DROP FUNCTION IF EXISTS banco.transfer_2pl(BIGINT, BIGINT, NUMERIC);
DROP FUNCTION IF EXISTS banco.ts_check_before_read(TEXT, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS banco.ts_check_before_write(TEXT, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS banco.transfer_ts(BIGINT, BIGINT, NUMERIC, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS banco.transfer_optimistic(BIGINT, BIGINT, NUMERIC);

-- =====================
-- 1) Two-Phase Locking (2PL) - advisory locks + lock_manager
-- =====================
CREATE OR REPLACE FUNCTION banco.recurso_hash_int(p_recurso TEXT) RETURNS BIGINT AS $$
DECLARE
    h BIGINT := 0;
    i INTEGER;
BEGIN
    FOR i IN 1..char_length(p_recurso) LOOP
        h := (h * 131 + ascii(substring(p_recurso, i, 1))) % 9223372036854775807;
    END LOOP;
    IF h = 0 THEN h := 1; END IF;
    RETURN h;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION banco.request_lock(p_recurso TEXT, p_tipo_lock VARCHAR DEFAULT 'EXCLUSIVE')
RETURNS VOID AS $$
DECLARE
    v_hash BIGINT;
BEGIN
    v_hash := banco.recurso_hash_int(p_recurso);
    INSERT INTO banco.lock_manager (recurso, recurso_hash, session_pid, tipo_lock, estado)
    VALUES (p_recurso, v_hash, pg_backend_pid(), p_tipo_lock, 'WAITING');

    PERFORM pg_advisory_lock(v_hash);

    UPDATE banco.lock_manager
    SET estado = 'HELD', released_at = NULL
    WHERE lm_id = (
        SELECT MAX(lm_id) FROM banco.lock_manager WHERE recurso_hash = v_hash AND session_pid = pg_backend_pid()
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION banco.release_lock(p_recurso TEXT)
RETURNS VOID AS $$
DECLARE
    v_hash BIGINT;
BEGIN
    v_hash := banco.recurso_hash_int(p_recurso);
    PERFORM pg_advisory_unlock(v_hash);
    UPDATE banco.lock_manager
    SET estado = 'RELEASED', released_at = now()
    WHERE recurso_hash = v_hash AND session_pid = pg_backend_pid() AND estado <> 'RELEASED';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION banco.transfer_2pl(p_from BIGINT, p_to BIGINT, p_amt NUMERIC)
RETURNS TEXT AS $$
DECLARE
    recurso_from TEXT := 'cuenta:' || p_from::TEXT;
    recurso_to   TEXT := 'cuenta:' || p_to::TEXT;
    v_saldo NUMERIC;
BEGIN
    IF p_from < p_to THEN
        PERFORM banco.request_lock(recurso_from);
        PERFORM banco.request_lock(recurso_to);
    ELSE
        PERFORM banco.request_lock(recurso_to);
        PERFORM banco.request_lock(recurso_from);
    END IF;

    SELECT saldo INTO v_saldo FROM banco.cuenta WHERE cuenta_id = p_from FOR UPDATE;
    IF v_saldo IS NULL THEN
        PERFORM banco.release_lock(recurso_from);
        PERFORM banco.release_lock(recurso_to);
        RETURN 'ABORTADO: CUENTA ORIGEN NO EXISTE';
    END IF;

    IF v_saldo < p_amt THEN
        INSERT INTO banco.transferencia (from_cuenta_id,to_cuenta_id,cantidad,tecnica,estado,nota)
        VALUES (p_from, p_to, p_amt, '2PL', 'ABORTADA', 'Fondos insuficientes (2PL)');
        PERFORM banco.release_lock(recurso_from);
        PERFORM banco.release_lock(recurso_to);
        RETURN 'ABORTADO: FONDOS';
    END IF;

    UPDATE banco.cuenta SET saldo = saldo - p_amt WHERE cuenta_id = p_from;
    UPDATE banco.cuenta SET saldo = saldo + p_amt WHERE cuenta_id = p_to;

    INSERT INTO banco.transferencia (from_cuenta_id,to_cuenta_id,cantidad,tecnica,estado,nota)
    VALUES (p_from, p_to, p_amt, '2PL', 'COMPLETADA', 'Transferencia con 2PL');

    PERFORM banco.release_lock(recurso_from);
    PERFORM banco.release_lock(recurso_to);
    RETURN 'COMPLETADA';
EXCEPTION WHEN OTHERS THEN
    PERFORM banco.release_lock(recurso_from);
    PERFORM banco.release_lock(recurso_to);
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- =====================
-- 2) Timestamp Ordering (TS)
-- =====================
CREATE OR REPLACE FUNCTION banco.ts_check_before_read(p_recurso TEXT, p_tx_ts TIMESTAMPTZ)
RETURNS BOOLEAN AS $$
DECLARE
    v_last_write TIMESTAMPTZ;
BEGIN
    SELECT last_write_ts INTO v_last_write FROM banco.timestamp_manager WHERE recurso = p_recurso;
    IF v_last_write IS NULL THEN
        INSERT INTO banco.timestamp_manager (recurso, last_read_ts)
        VALUES (p_recurso, p_tx_ts)
        ON CONFLICT (recurso) DO UPDATE
        SET last_read_ts = GREATEST(COALESCE(banco.timestamp_manager.last_read_ts, p_tx_ts), p_tx_ts);
        RETURN TRUE;
    END IF;

    IF v_last_write > p_tx_ts THEN
        RETURN FALSE;
    ELSE
        UPDATE banco.timestamp_manager
        SET last_read_ts = GREATEST(COALESCE(last_read_ts, p_tx_ts), p_tx_ts)
        WHERE recurso = p_recurso;
        RETURN TRUE;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION banco.ts_check_before_write(p_recurso TEXT, p_tx_ts TIMESTAMPTZ)
RETURNS BOOLEAN AS $$
DECLARE
    v_last_read TIMESTAMPTZ;
BEGIN
    SELECT last_read_ts INTO v_last_read FROM banco.timestamp_manager WHERE recurso = p_recurso;
    IF v_last_read IS NOT NULL AND v_last_read > p_tx_ts THEN
        RETURN FALSE;
    END IF;

    INSERT INTO banco.timestamp_manager (recurso, last_write_ts)
    VALUES (p_recurso, p_tx_ts)
    ON CONFLICT (recurso) DO UPDATE
    SET last_write_ts = GREATEST(COALESCE(banco.timestamp_manager.last_write_ts, p_tx_ts), p_tx_ts);

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION banco.transfer_ts(p_from BIGINT, p_to BIGINT, p_amt NUMERIC, p_tx_ts TIMESTAMPTZ)
RETURNS TEXT AS $$
DECLARE
    recurso_from TEXT := 'cuenta:' || p_from::TEXT;
    recurso_to   TEXT := 'cuenta:' || p_to::TEXT;
    v_saldo NUMERIC;
    ok BOOLEAN;
BEGIN
    ok := banco.ts_check_before_read(recurso_from, p_tx_ts);
    IF NOT ok THEN
        RETURN 'ABORTADO: TS READ FAIL (origen)';
    END IF;
    ok := banco.ts_check_before_read(recurso_to, p_tx_ts);
    IF NOT ok THEN
        RETURN 'ABORTADO: TS READ FAIL (destino)';
    END IF;

    SELECT saldo INTO v_saldo FROM banco.cuenta WHERE cuenta_id = p_from;
    IF v_saldo IS NULL THEN
        RETURN 'ABORTADO: CUENTA ORIGEN NO EXISTE';
    END IF;
    IF v_saldo < p_amt THEN
        RETURN 'ABORTADO: FONDOS';
    END IF;

    ok := banco.ts_check_before_write(recurso_from, p_tx_ts);
    IF NOT ok THEN
        RETURN 'ABORTADO: TS WRITE FAIL (origen)';
    END IF;
    ok := banco.ts_check_before_write(recurso_to, p_tx_ts);
    IF NOT ok THEN
        RETURN 'ABORTADO: TS WRITE FAIL (destino)';
    END IF;

    UPDATE banco.cuenta SET saldo = saldo - p_amt WHERE cuenta_id = p_from;
    UPDATE banco.cuenta SET saldo = saldo + p_amt WHERE cuenta_id = p_to;

    INSERT INTO banco.transferencia (from_cuenta_id,to_cuenta_id,cantidad,tecnica,estado,nota)
    VALUES (p_from, p_to, p_amt, 'TS', 'COMPLETADA', 'Transferencia con TS');

    RETURN 'COMPLETADA';
END;
$$ LANGUAGE plpgsql;

-- =====================
-- 3) Optimistic concurrency control
-- =====================
CREATE OR REPLACE FUNCTION banco.transfer_optimistic(p_from BIGINT, p_to BIGINT, p_amt NUMERIC)
RETURNS TEXT AS $$
DECLARE
    rec_from RECORD;
    rec_to RECORD;
    updated_from INT;
    updated_to INT;
BEGIN
    SELECT cuenta_id, saldo, version INTO rec_from FROM banco.cuenta WHERE cuenta_id = p_from;
    SELECT cuenta_id, saldo, version INTO rec_to   FROM banco.cuenta WHERE cuenta_id = p_to;

    IF rec_from.cuenta_id IS NULL OR rec_to.cuenta_id IS NULL THEN
        RETURN 'ABORTADO: CUENTA NO EXISTE';
    END IF;

    IF rec_from.saldo < p_amt THEN
        RETURN 'ABORTADO: FONDOS';
    END IF;

    UPDATE banco.cuenta
    SET saldo = saldo - p_amt, version = version + 1
    WHERE cuenta_id = p_from AND version = rec_from.version;
    GET DIAGNOSTICS updated_from = ROW_COUNT;

    IF updated_from = 0 THEN
        RETURN 'ABORT: VERSION CONFLICT FROM';
    END IF;

    UPDATE banco.cuenta
    SET saldo = saldo + p_amt, version = version + 1
    WHERE cuenta_id = p_to AND version = rec_to.version;
    GET DIAGNOSTICS updated_to = ROW_COUNT;

    IF updated_to = 0 THEN
        -- revertir primera actualizacion
        UPDATE banco.cuenta
        SET saldo = saldo + p_amt, version = version + 1
        WHERE cuenta_id = p_from;
        RETURN 'ABORT: VERSION CONFLICT TO (compensado)';
    END IF;

    INSERT INTO banco.transferencia (from_cuenta_id,to_cuenta_id,cantidad,tecnica,estado,nota)
    VALUES (p_from, p_to, p_amt, 'OPTIMISTIC', 'COMPLETADA', 'Transferencia con control optimista');

    RETURN 'COMPLETADA';
END;
$$ LANGUAGE plpgsql;

-- =====================
-- 4) Consultas de evidencia (ejecutar manualmente cuando quieras)
-- =====================
SELECT * FROM banco.lock_manager ORDER BY created_at DESC LIMIT 100;
SELECT * FROM banco.timestamp_manager ORDER BY ts_id DESC LIMIT 100;
SELECT * FROM banco.transferencia ORDER BY created_at DESC LIMIT 100;
SELECT * FROM banco.cuenta ORDER BY cuenta_id;
