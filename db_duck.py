# db_duck.py - Funções para gerenciamento do DuckDB
import pandas as pd
import duckdb
from pathlib import Path
import logging
from typing import Dict, Any

# Configurar logger
_logger = logging.getLogger("db_duck")

def ensure_db(db_path: str | Path) -> bool:
    """
    Garante que o banco DuckDB existe e tem as tabelas necessárias.
    Retorna True se bem-sucedido.
    """
    try:
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        con = duckdb.connect(str(db_path))
        
        # Tabelas básicas do sistema UniVolei
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS rallies (
                rally_id INTEGER PRIMARY KEY,
                match_id INTEGER,
                set_number INTEGER,
                rally_no INTEGER,
                player_number INTEGER,
                action VARCHAR,
                result VARCHAR,
                who_scored VARCHAR,
                score_home INTEGER,
                score_away INTEGER,
                raw_text VARCHAR,
                court_x DOUBLE,
                court_y DOUBLE,
                position_zone VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sets (
                set_id INTEGER PRIMARY KEY,
                match_id INTEGER,
                set_number INTEGER,
                home_points INTEGER,
                away_points INTEGER,
                winner_team_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS amistosos (
                match_id INTEGER PRIMARY KEY,
                away_team_id INTEGER,
                date DATE,
                home_sets INTEGER,
                away_sets INTEGER,
                is_closed BOOLEAN DEFAULT FALSE,
                closed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS equipes (
                team_id INTEGER PRIMARY KEY,
                team_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS jogadoras (
                player_id INTEGER PRIMARY KEY,
                team_id INTEGER,
                player_number INTEGER,
                player_name VARCHAR,
                position VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        for sql in tables_sql:
            con.execute(sql)
        
        con.close()
        _logger.info(f"DuckDB inicializado: {db_path}")
        return True
        
    except Exception as e:
        _logger.error(f"Erro ao inicializar DuckDB: {e}")
        return False

def replace_all(db_path: str | Path, frames: Dict[str, pd.DataFrame]) -> bool:
    """
    Substitui todas as tabelas no DuckDB pelos DataFrames fornecidos.
    """
    try:
        db_path = Path(db_path)
        con = duckdb.connect(str(db_path))
        
        for table_name, df in frames.items():
            if df is not None and not df.empty:
                # Remove tabela existente e recria com dados novos
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Registra DataFrame temporariamente
                con.register("temp_df", df)
                
                # Cria tabela a partir do DataFrame
                con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                
                # Remove registro temporário
                con.unregister("temp_df")
                
                _logger.debug(f"Tabela {table_name} atualizada com {len(df)} registros")
        
        # Força checkpoint para garantir persistência
        con.execute("CHECKPOINT")
        con.close()
        
        _logger.info(f"DuckDB atualizado: {len(frames)} tabelas")
        return True
        
    except Exception as e:
        _logger.error(f"Erro ao atualizar DuckDB: {e}")
        return False

def load_all(db_path: str | Path) -> Dict[str, pd.DataFrame]:
    """
    Carrega todas as tabelas do DuckDB para DataFrames.
    """
    frames = {}
    try:
        db_path = Path(db_path)
        if not db_path.exists():
            _logger.warning(f"Arquivo DuckDB não encontrado: {db_path}")
            return frames
            
        con = duckdb.connect(str(db_path))
        
        # Lista todas as tabelas
        tables_result = con.execute("SHOW TABLES").fetchall()
        tables = [table[0] for table in tables_result]
        
        for table in tables:
            try:
                df = con.execute(f"SELECT * FROM {table}").df()
                frames[table] = df
                _logger.debug(f"Tabela {table} carregada: {len(df)} registros")
            except Exception as e:
                _logger.warning(f"Erro ao carregar tabela {table}: {e}")
        
        con.close()
        _logger.info(f"DuckDB carregado: {len(frames)} tabelas")
        
    except Exception as e:
        _logger.error(f"Erro ao carregar DuckDB: {e}")
    
    return frames

def backup_db(source_path: str | Path, backup_dir: str | Path = None) -> bool:
    """
    Cria backup do banco DuckDB.
    """
    try:
        source_path = Path(source_path)
        if not source_path.exists():
            _logger.warning(f"Arquivo fonte não encontrado: {source_path}")
            return False
            
        if backup_dir is None:
            backup_dir = source_path.parent / "backups"
        backup_dir = Path(backup_dir)
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"volei_base_dados_{timestamp}.duckdb"
        
        # Conecta e exporta
        con = duckdb.connect(str(source_path))
        con.execute(f"EXPORT DATABASE '{backup_path}' (FORMAT PARQUET)")
        con.close()
        
        _logger.info(f"Backup criado: {backup_path}")
        return True
        
    except Exception as e:
        _logger.error(f"Erro ao criar backup: {e}")
        return False

# Funções auxiliares para compatibilidade com o código existente
def sync_frames_to_duck(frames: Dict[str, pd.DataFrame], duck_path: str | Path) -> str:
    """
    Sincroniza frames para DuckDB (usada no sistema de persistência).
    Retorna string de status.
    """
    try:
        success = replace_all(duck_path, frames)
        if success:
            return f"DUCKDB: ok -> {duck_path}"
        else:
            return f"DUCKDB: erro na sincronização"
    except Exception as e:
        return f"DUCKDB: erro {str(e)}"

def get_db_stats(db_path: str | Path) -> Dict[str, Any]:
    """
    Retorna estatísticas do banco DuckDB.
    """
    stats = {}
    try:
        db_path = Path(db_path)
        if not db_path.exists():
            return {"error": "Arquivo não encontrado"}
            
        con = duckdb.connect(str(db_path))
        
        # Tamanho do arquivo
        stats["file_size_mb"] = db_path.stat().st_size / (1024 * 1024)
        
        # Contagem de tabelas e registros
        tables_result = con.execute("SHOW TABLES").fetchall()
        stats["table_count"] = len(tables_result)
        
        table_stats = {}
        for table in tables_result:
            table_name = table[0]
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            table_stats[table_name] = count
        
        stats["table_record_counts"] = table_stats
        stats["total_records"] = sum(table_stats.values())
        
        con.close()
        
    except Exception as e:
        stats["error"] = str(e)
    
    return stats

# Teste básico se executado diretamente
if __name__ == "__main__":
    # Configurar logging básico para teste
    logging.basicConfig(level=logging.INFO)
    
    # Teste das funções
    test_db = Path("test_db.duckdb")
    
    print("=== Teste db_duck ===")
    
    # 1. Garantir que o DB existe
    print("1. Inicializando DB...")
    success = ensure_db(test_db)
    print(f"   Resultado: {success}")
    
    # 2. Criar alguns dados de teste
    print("2. Criando dados de teste...")
    test_frames = {
        "rallies": pd.DataFrame([{
            "rally_id": 1, "match_id": 1, "set_number": 1, "rally_no": 1,
            "player_number": 10, "action": "DIAGONAL", "result": "PONTO",
            "who_scored": "NOS", "score_home": 1, "score_away": 0
        }]),
        "amistosos": pd.DataFrame([{
            "match_id": 1, "away_team_id": 2, "date": "2024-01-01",
            "home_sets": 0, "away_sets": 0, "is_closed": False
        }])
    }
    
    # 3. Sincronizar dados
    print("3. Sincronizando dados...")
    success = replace_all(test_db, test_frames)
    print(f"   Resultado: {success}")
    
    # 4. Carregar dados
    print("4. Carregando dados...")
    loaded_frames = load_all(test_db)
    print(f"   Tabelas carregadas: {list(loaded_frames.keys())}")
    
    # 5. Estatísticas
    print("5. Estatísticas...")
    stats = get_db_stats(test_db)
    print(f"   Estatísticas: {stats}")
    
    # 6. Limpar teste
    if test_db.exists():
        test_db.unlink()
    
    print("=== Teste concluído ===")