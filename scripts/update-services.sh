#!/bin/bash

set -e

echo "🔄 Updating services on nodes Swarm..."


RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

update_service() {
    local service_name=$1
    local image_name=$2
    
    echo -e "${YELLOW}Service update ${service_name}...${NC}"
    if docker service inspect "${service_name}" >/dev/null 2>&1; then
        docker service update --image "${image_name}" --force "${service_name}"
        echo -e "${GREEN}✅ Service ${service_name} updated!${NC}"
    else
        echo -e "${RED}❌ Service ${service_name} not found!${NC}"
    fi
}

echo -e "${YELLOW}🔍 Checking for local images...${NC}"
if ! docker image inspect bc-server:latest >/dev/null 2>&1; then
    echo -e "${RED}❌ Image bc-server:latest not found! Please run first build-images-local.sh${NC}"
    exit 1
fi

if ! docker image inspect db-controller:latest >/dev/null 2>&1; then
    echo -e "${RED}❌ Image db-controller:latest not found! Please run first build-images-local.sh${NC}"
    exit 1
fi

if ! docker image inspect tg-bot:latest >/dev/null 2>&1; then
    echo -e "${RED}❌ Image tg-bot:latest not found! Please run firstbuild-images-local.sh${NC}"
    exit 1
fi

echo -e "${GREEN}✅ All images found locally${NC}"


update_service "tgbot_bc-server" "bc-server:latest"
update_service "tgbot_db-controller-go" "db-controller:latest" 
update_service "tgbot_tg-bot" "tg-bot:latest"

echo -e "\n${GREEN}🎉 All services have been updated!${NC}"
echo -e "${YELLOW}Service status:${NC}"
docker service ls | grep -E "(bc-server|db-controller|tg-bot)"