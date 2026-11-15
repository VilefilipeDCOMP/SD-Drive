"""
tests/conftest.py

Configurações globais do pytest.
Adiciona src/ ao sys.path para imports sem instalar o pacote.
"""
import sys
import os

# Garante que o diretório raiz do projeto esteja no Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
