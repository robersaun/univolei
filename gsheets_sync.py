# gsheets_sync.py - Sincronização com Google Sheets
import pandas as pd
import streamlit as st
import logging
from datetime import datetime
import json
from typing import Optional, Dict


# Configurar logger
_logger = logging.getLogger("gsheets_sync")

def is_enabled():
    """
    Verifica se a sincronização com Google Sheets está habilitada.
    """
    try:
        # Tenta carregar configurações do Streamlit secrets
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            return True
        
        # Verifica variáveis de ambiente
        import os
        if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            return True
            
        # Verifica config.ini via código existente no index.py
        from pathlib import Path
        import configparser
        
        config_path = Path(__file__).parent / "config.ini"
        if config_path.exists():
            config = configparser.ConfigParser()
            config.read(config_path)
            if (config.has_section('online') and 
                config.get('online', 'gsheet_id', fallback='').strip()):
                return True
                
        return False
        
    except Exception as e:
        _logger.warning(f"Erro ao verificar se Google Sheets está habilitado: {e}")
        return False

def _get_gspread_client():
    """
    Retorna cliente gspread autenticado (código do index.py).
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # 1) Streamlit secrets
        try:
            if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
                sa_info = st.secrets["gcp_service_account"]
                if isinstance(sa_info, str):
                    sa_info = json.loads(sa_info)
                creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
                return gspread.authorize(creds)
        except Exception as e:
            _logger.warning(f"gspread via st.secrets falhou: {e}")

        # 2) Variável de ambiente
        try:
            import os
            cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
            if cred_path:
                creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
                return gspread.authorize(creds)
        except Exception as e:
            _logger.warning(f"gspread via env var falhou: {e}")

        # 3) config.ini
        try:
            from pathlib import Path
            import configparser
            
            config_path = Path(__file__).parent / "config.ini"
            if config_path.exists():
                config = configparser.ConfigParser()
                config.read(config_path)
                
                if config.has_section('gcp'):
                    mode = config.get('gcp', 'credentials_mode', fallback='').strip().lower()
                    if mode == 'path':
                        cred_path = config.get('gcp', 'credentials_path', fallback='').strip()
                        if cred_path:
                            creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
                            return gspread.authorize(creds)
                    elif mode == 'inline':
                        inline_json = config.get('gcp', 'inline_json', fallback='').strip()
                        if inline_json:
                            sa_info = json.loads(inline_json)
                            creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
                            return gspread.authorize(creds)
        except Exception as e:
            _logger.warning(f"gspread via config.ini falhou: {e}")

        return None
        
    except Exception as e:
        _logger.error(f"Erro ao obter cliente gspread: {e}")
        return None

def _normalize_gsheet_id(raw_id):
    """
    Normaliza ID da planilha (código do index.py).
    """
    s = (raw_id or "").strip()
    if not s:
        return ""
    
    # Se for URL completa, extrai o ID
    if "spreadsheets/d/" in s:
        return s.split("spreadsheets/d/")[1].split("/")[0]
    
    # Se for pasta/Doc/Slide, invalida
    if any(x in s for x in ["/drive/folders/", "/document/d/", "/presentation/d/"]):
        return ""
    
    return s

def _get_spreadsheet_id():
    """
    Obtém o ID da planilha das configurações.
    """
    try:
        # 1) Streamlit secrets
        if hasattr(st, 'secrets'):
            if 'gsheet_id' in st.secrets:
                return _normalize_gsheet_id(st.secrets['gsheet_id'])
            if 'online' in st.secrets and 'gsheet_id' in st.secrets['online']:
                return _normalize_gsheet_id(st.secrets['online']['gsheet_id'])
        
        # 2) config.ini
        from pathlib import Path
        import configparser
        
        config_path = Path(__file__).parent / "config.ini"
        if config_path.exists():
            config = configparser.ConfigParser()
            config.read(config_path)
            if config.has_section('online'):
                gsheet_id = config.get('online', 'gsheet_id', fallback='').strip()
                return _normalize_gsheet_id(gsheet_id)
                
        return None
        
    except Exception as e:
        _logger.error(f"Erro ao obter spreadsheet ID: {e}")
        return None

def _prepare_dataframe_for_sheets(df):
    """
    Prepara DataFrame para envio ao Google Sheets.
    """
    if df is None or df.empty:
        return [["Sem dados"]]
    
    # Converte todos os valores para string e trata NaN
    df_clean = df.astype(object).where(pd.notna(df), "").astype(str)
    
    # Cabeçalho + dados
    values = [df_clean.columns.tolist()]
    values.extend(df_clean.values.tolist())
    
    return values

def read_all(sheet_map: Optional[Dict[str, str]] = None):
    """
    Lê todas as abas da planilha e retorna um dict[str, pd.DataFrame].
    Se 'sheet_map' for fornecido, usa o mapeamento {nome_tabela: nome_aba}.
    Caso contrário, lê todas as worksheets e usa seus títulos como chaves.
    """
    import pandas as pd
    frames: dict[str, pd.DataFrame] = {}

    if not is_enabled():
        raise RuntimeError("Google Sheets não habilitado")

    spreadsheet_id = _get_spreadsheet_id()
    if not spreadsheet_id:
        raise RuntimeError("ID da planilha não configurado")

    client = _get_gspread_client()
    if not client:
        raise RuntimeError("Falha na autenticação do Google Sheets")

    spreadsheet = client.open_by_key(spreadsheet_id)

    if sheet_map:
        # Lê apenas as abas mapeadas
        for table_name, ws_title in sheet_map.items():
            try:
                ws = spreadsheet.worksheet(ws_title)
                values = ws.get_all_values()
                if not values:
                    frames[table_name] = pd.DataFrame()
                    continue
                header, data = values[0], values[1:]
                df = pd.DataFrame(data, columns=header)
                frames[table_name] = df
            except Exception:
                frames[table_name] = pd.DataFrame()
    else:
        # Lê todas as abas
        for ws in spreadsheet.worksheets():
            try:
                values = ws.get_all_values()
                if not values:
                    frames[ws.title] = pd.DataFrame()
                    continue
                header, data = values[0], values[1:]
                df = pd.DataFrame(data, columns=header)
                frames[ws.title] = df
            except Exception:
                frames[ws.title] = pd.DataFrame()

    return frames

def _sync_all(frames):
    """
    Sincroniza todos os frames com Google Sheets.
    Retorna string de status.
    """
    try:
        if not is_enabled():
            return "Google Sheets não habilitado"
        
        spreadsheet_id = _get_spreadsheet_id()
        if not spreadsheet_id:
            return "ID da planilha não configurado"
        
        client = _get_gspread_client()
        if not client:
            return "Falha na autenticação do Google Sheets"
        
        # Abre a planilha
        try:
            spreadsheet = client.open_by_key(spreadsheet_id)
        except Exception as e:
            error_msg = str(e)
            if "This operation is not supported for this document" in error_msg:
                return "ID aponta para documento não suportado (use planilha nativa do Google Sheets)"
            elif "Unable to open spreadsheet" in error_msg or "not found" in error_msg.lower():
                return "Planilha não encontrada - verifique o ID e as permissões"
            else:
                return f"Erro ao abrir planilha: {error_msg}"
        
        # Sincroniza cada tabela
        results = []
        for table_name, df in frames.items():
            if df is None or not isinstance(df, pd.DataFrame):
                continue
                
            try:
                # Normaliza nome da aba
                sheet_title = str(table_name)[:95].replace("/", "_").replace("\\", "_").replace(":", " ")
                
                # Obtém ou cria a aba
                try:
                    worksheet = spreadsheet.worksheet(sheet_title)
                except Exception:
                    # Cria nova aba
                    rows = max(1000, len(df) + 100)
                    cols = max(26, len(df.columns) + 10)
                    worksheet = spreadsheet.add_worksheet(title=sheet_title, rows=rows, cols=cols)
                
                # Limpa a aba
                worksheet.clear()
                
                # Prepara e envia dados
                values = _prepare_dataframe_for_sheets(df)
                
                # Ajusta tamanho da planilha se necessário
                try:
                    worksheet.resize(rows=max(100, len(values) + 50), 
                                   cols=max(26, len(values[0]) if values else 10))
                except Exception:
                    pass  # Algumas APIs não permitem resize
                
                # Envia dados
                if values:
                    worksheet.update(values, value_input_option="RAW")
                
                results.append(f"{table_name}: ✓")
                
            except Exception as e:
                results.append(f"{table_name}: ✗ ({str(e)})")
                _logger.error(f"Erro ao sincronizar {table_name}: {e}")
        
        if results:
            return f"Google Sheets sincronizado: {', '.join(results)}"
        else:
            return "Nenhuma tabela sincronizada"
            
    except Exception as e:
        error_msg = f"Erro geral na sincronização: {e}"
        _logger.error(error_msg)
        return error_msg

def sync_table(df, table_name, spreadsheet_id=None):
    """
    Sincroniza uma tabela específica com Google Sheets.
    """
    try:
        if not spreadsheet_id:
            spreadsheet_id = _get_spreadsheet_id()
        
        if not spreadsheet_id:
            return "ID da planilha não configurado"
        
        client = _get_gspread_client()
        if not client:
            return "Falha na autenticação"
        
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # Normaliza nome da aba
        sheet_title = str(table_name)[:95].replace("/", "_").replace("\\", "_").replace(":", " ")
        
        # Obtém ou cria a aba
        try:
            worksheet = spreadsheet.worksheet(sheet_title)
        except Exception:
            rows = max(1000, len(df) + 100)
            cols = max(26, len(df.columns) + 10)
            worksheet = spreadsheet.add_worksheet(title=sheet_title, rows=rows, cols=cols)
        
        # Limpa e atualiza
        worksheet.clear()
        values = _prepare_dataframe_for_sheets(df)
        
        if values:
            worksheet.update(values, value_input_option="RAW")
        
        return f"Tabela {table_name} sincronizada com sucesso"
        
    except Exception as e:
        error_msg = f"Erro ao sincronizar {table_name}: {e}"
        _logger.error(error_msg)
        return error_msg

def get_sheet_url():
    """
    Retorna a URL da planilha se configurada.
    """
    spreadsheet_id = _get_spreadsheet_id()
    if spreadsheet_id:
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    return None

def test_connection():
    """
    Testa a conexão com Google Sheets.
    Retorna (sucesso, mensagem).
    """
    try:
        if not is_enabled():
            return False, "Google Sheets não habilitado"
        
        spreadsheet_id = _get_spreadsheet_id()
        if not spreadsheet_id:
            return False, "ID da planilha não configurado"
        
        client = _get_gspread_client()
        if not client:
            return False, "Falha na autenticação"
        
        # Tenta abrir a planilha
        spreadsheet = client.open_by_key(spreadsheet_id)
        title = spreadsheet.title
        
        return True, f"Conexão bem-sucedida: {title}"
        
    except Exception as e:
        error_msg = str(e)
        if "not found" in error_msg.lower():
            return False, "Planilha não encontrada - verifique o ID"
        elif "permission" in error_msg.lower():
            return False, "Sem permissão para acessar a planilha"
        else:
            return False, f"Erro na conexão: {error_msg}"

# Função de compatibilidade com código existente
def sync_to_gsheets(frames, reason="manual"):
    """
    Função compatível com o código do index.py.
    """
    return _sync_all(frames)



# Teste básico
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=== Teste gsheets_sync ===")
    
    # Testa se está habilitado
    enabled = is_enabled()
    print(f"1. Google Sheets habilitado: {enabled}")
    
    if enabled:
        # Testa conexão
        success, message = test_connection()
        print(f"2. Teste de conexão: {success} - {message}")
        
        # Testa URL
        url = get_sheet_url()
        print(f"3. URL da planilha: {url}")
    
    print("=== Teste concluído ===")