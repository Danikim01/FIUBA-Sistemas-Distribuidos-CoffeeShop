#!/bin/bash
# Script para ejecutar los tests del TPV sharded worker

set -e

# Colores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Directorio base
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${YELLOW}=== Ejecutando tests para TPV Sharded Worker ===${NC}"

# Verificar que pytest esté instalado
if ! command -v pytest &> /dev/null; then
    echo -e "${RED}Error: pytest no está instalado${NC}"
    echo "Instala con: pip install pytest pytest-timeout"
    exit 1
fi

# Opciones por defecto
TEST_TYPE="${1:-all}"
VERBOSITY="${2:-v}"

cd "$PROJECT_ROOT"

# Ejecutar tests según el tipo
case "$TEST_TYPE" in
    "unit"|"comprehensive")
        echo -e "${GREEN}Ejecutando tests unitarios/comprehensivos...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY
        ;;
    "state")
        echo -e "${GREEN}Ejecutando tests de estado...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY -m state
        ;;
    "duplicate")
        echo -e "${GREEN}Ejecutando tests de mensajes duplicados...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY -m duplicate
        ;;
    "eof")
        echo -e "${GREEN}Ejecutando tests de EOF...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY -m eof
        ;;
    "race")
        echo -e "${GREEN}Ejecutando tests de race conditions...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY -m race
        ;;
    "failure")
        echo -e "${GREEN}Ejecutando tests de fallos...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY -m failure
        ;;
    "all")
        echo -e "${GREEN}Ejecutando todos los tests...${NC}"
        pytest tests/test_tpv_sharded_comprehensive.py -$VERBOSITY
        ;;
    *)
        echo -e "${RED}Opción desconocida: $TEST_TYPE${NC}"
        echo "Uso: $0 [unit|state|duplicate|eof|race|failure|all] [v|vv|vvv]"
        exit 1
        ;;
esac

echo -e "${GREEN}=== Tests completados ===${NC}"

