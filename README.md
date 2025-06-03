# Bulls and Cows Game - Telegram Bot

## Project Overview

This is a Telegram bot implementation of the classic "Bulls and Cows" number guessing game. The project features:

- Multiplayer game modes (single player, vs bot, random opponent)
- Multiple difficulty levels for bot opponents
- Multi-language support (English and Russian)
- Persistent game state using PostgreSQL
- Event-driven architecture with Kafka
- Containerized deployment with Docker

## System Architecture

The system consists of several components:

1. Telegram Bot (tg-bot) - Handles user interactions and game flow
2. Game Server (bc-server) - Implements game logic and rules
3. Database Controller (db-controller-go) - Manages persistence in PostgreSQL
4. Message Broker (Kafka) - Handles communication between components
5. Database (PostgreSQL) - Stores game state and player data

## Prerequisites

Before you begin, ensure you have the following installed:

- Docker
- Docker Compose
- Git
- Telegram account (for testing the bot)

## 1. Create environment files

Create a .env file in the project root with the following content:

### Configuration
TG_API_BC_TOKEN=_your telegram bot token_ 
POSTGRES_USER=_username for admin_ 
POSTGRES_PASSWORD=_your secure password_ 
ADMIN_EMAIL=_your email_ 

## 2. Build and start the containers

docker-compose up -d --build
This will:
- Set up a Kafka cluster with Zookeeper
- Create a PostgreSQL database
- Build and start all application components
- Create necessary Kafka topics

## 3. Verify the installation

Check that all containers are running:

docker-compose ps
You can access:
- Kafka UI at http://localhost:8080
- pgAdmin at http://localhost:5050 (login with the email and password from your .env file)

## How to Use the Bot

1. Search for your bot in Telegram using the bot username
2. Start the bot with the /start command
3. Use the menu to:
   - Start a new game (single player, vs bot, or random opponent)
   - View game rules
   - Change language (English/Russian)
   - Provide feedback

## Game Modes

1. Single Player - Play against yourself to practice
2. Versus Bot - Play against AI with selectable difficulty:
   - Easy
   - Medium
   - Hard
3. Random Opponent - Play against another random player

## Game Rules

The game "Bulls and Cows" is a number guessing game where:
- The computer (or opponent) thinks of a 4-digit number with no repeating digits
- You try to guess the number
- For each guess, you get feedback:
  - "Bulls" = correct digit in the correct position
  - "Cows" = correct digit in the wrong position

Example:
- Secret number: 1234
- Your guess: 1359
- Result: 1 Bull (digit 1) and 1 Cow (digit 3)

## Development

### Project Structure

- db-controller/ - Go service for database operations
- tg-bot/ - Python Telegram bot implementation
- docker-compose.yaml - Container orchestration configuration

## Troubleshooting
If services fail to start:
1. Check logs for specific containers:
  
   docker-compose logs <service_name>
   
2. Ensure all environment variables are set correctly
3. Verify Kafka topics were created (check in Kafka UI)

## License

This project is open-source. Feel free to use and modify it according to your needs.