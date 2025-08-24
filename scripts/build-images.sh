#!/bin/bash

set -e  # Exit with error

echo "🐳 Build Docker images locally..."

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color


check_success() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Successfully!${NC}"
    else
        echo -e "${RED}❌ Error!${NC}"
        exit 1
    fi
}

# 1. Build bc-server
echo -e "\n${YELLOW}🔄 Build image bc-server:latest${NC}"
docker build -t bc-server:latest https://github.com/Katton1301/BullsAndCows.git#master
check_success

# 2. Build db-controller-go
echo -e "\n${YELLOW}🔄 Build image db-controller:latest${NC}"
docker build -t db-controller:latest ../db-controller
check_success

# 3. Build tg-bot
echo -e "\n${YELLOW}🔄 Build image tg-bot:latest${NC}"
docker build -t tg-bot:latest ../tg-bot
check_success

# 4. Check images
echo -e "\n${YELLOW}🔍 Check for update images:${NC}"
docker images | grep -E "(bc-server|db-controller|tg-bot)"

echo -e "\n${GREEN}🎉 All images successfully built locally!${NC}"
echo -e "${YELLOW}To update on nodes, run:${NC}"
echo -e "  ./update-services.sh"