package main

import (
  "context"
  "encoding/json"
  "fmt"
  "log"
  "math/rand"
  "os"
  "strconv"
  "time"
  "strings"

  "github.com/confluentinc/confluent-kafka-go/v2/kafka"
  "github.com/jackc/pgx/v5"
)

var computerNames map[string][]string
var lobbyPlayerLimit int

type PlayerData struct {
  ID   int64  `json:"player_id"`
  FirstName string `json:"firstname"`
  LastName string `json:"lastname"`
  FullName string `json:"fullname"`
  UserName string `json:"username"`
  Lang string `json:"lang"`
  State string `json:"state"`
}

type GameData struct {
  ID          int64  `json:"id"`
  ServerId    int64  `json:"server_id"`
  Mode        string `json:"mode"`
  Stage       string `json:"stage"`
  Step        int    `json:"step"`
  SecretValue int    `json:"secret_value"`
}

type PlayerGameData struct {
  PlayerId   int64  `json:"player_id"`
  ServerId int64 `json:"server_id"`
  GameId int64 `json:"game_id"`
  IsCurrentGame bool `json:"is_current_game"`
  IsHost bool `json:"is_host"`
}

type ComputerGameData struct {
    ComputerId int64  `json:"computer_id"`
    PlayerId   int64  `json:"player_id"`
    ServerId   int64  `json:"server_id"`
    GameId     int64  `json:"game_id"`
    GameBrain  string `json:"game_brain"`
    Name       string `json:"name,omitempty"`
}

type GamesHistoryData struct {
  GameId int64 `json:"game_id"`
  PlayerId   int64  `json:"player_id"`
  ServerId int64 `json:"server_id"`
  Step  int `json:"step"`
  GameValue int `json:"game_value"`
  Bulls int `json:"bulls"`
  Cows int `json:"cows"`
  IsComputer bool `json:"is_computer"`
  IsGiveUp bool `json:"is_give_up"`
  Timestamp time.Time `json:"timestamp"`
}

type GenericResponse struct {
  CorrelationId string      `json:"correlation_id"`
  Answer        string      `json:"answer"`
  Error         string      `json:"error"`
  Data          interface{} `json:"data,omitempty"`
}

type FeedBackData struct {
    UserName string `json:"username"`
    Message string `json:"message"`
}

type NameData struct {
    ID       int64  `json:"id"`
    IsPlayer bool   `json:"is_player"`
    Name     string `json:"name"`
}

type CurrentPlayerInfo struct {
    PlayerId int64 `json:"player_id"`
    Finished bool  `json:"finished"`
    GiveUp   bool  `json:"give_up"`
}

type LobbyData struct {
    ID          int64  `json:"id"`
    ServerId    int64  `json:"server_id"`
    HostId   int64  `json:"host_id"`
    GameId   int64  `json:"game_id"`
    IsPrivate   bool   `json:"is_private"`
    Password    string `json:"password,omitempty"`
    Status      string `json:"status"` // WAITING, STARTED, FINISHED
}

type LobbyPlayerData struct {
    LobbyId    int64  `json:"lobby_id"`
    PlayerId   int64  `json:"player_id"`
    IsReady    bool   `json:"is_ready"`
    JoinedAt   time.Time `json:"joined_at"`
    Host      bool   `json:"host"`
    Access    bool   `json:"access"`
    InLobby   bool   `json:"in_lobby"`
}

type BlacklistData struct {
    Id         int64     `json:"id"`
    LobbyId    int64     `json:"lobby_id"`
    PlayerId   int64     `json:"player_id"`
    BannedById int64     `json:"banned_by_id"`
    Reason     string    `json:"reason"`
    BannedAt   time.Time `json:"banned_at"`
}

type KafkaMessage struct {
  Command string          `json:"command"`
  CorrelationId string      `json:"correlation_id"`
  Data    json.RawMessage `json:"data"`
}

func (g *GamesHistoryData) UnmarshalJSON(data []byte) error {
    type Alias GamesHistoryData
    aux := &struct {
        Timestamp string `json:"timestamp"`
        *Alias
    }{
        Alias: (*Alias)(g),
    }

    if err := json.Unmarshal(data, &aux); err != nil {
        return err
    }

    parsedTime, err := time.Parse(time.RFC3339Nano, aux.Timestamp)
    if err != nil {
        parsedTime, err = time.Parse("2006-01-02T15:04:05", aux.Timestamp)
        if err != nil {
            return err
        }
    }

    g.Timestamp = parsedTime
    return nil
}

func loadComputerNames() error {
    file, err := os.ReadFile("computer_names.json")
    if err != nil {
        return fmt.Errorf("failed to read computer names file: %w", err)
    }

    err = json.Unmarshal(file, &computerNames)
    if err != nil {
        return fmt.Errorf("failed to parse computer names: %w", err)
    }

    return nil
}

func loadLobbyPlayerLimit() {
    limitStr := os.Getenv("LOBBY_PLAYER_LIMIT")
    if limitStr == "" {
        lobbyPlayerLimit = 10 // Default limit
        log.Printf("LOBBY_PLAYER_LIMIT not set, using default: %d", lobbyPlayerLimit)
        return
    }

    limit, err := strconv.Atoi(limitStr)
    if err != nil || limit <= 0 {
        lobbyPlayerLimit = 10 // Default limit on invalid value
        log.Printf("Invalid LOBBY_PLAYER_LIMIT value '%s', using default: %d", limitStr, lobbyPlayerLimit)
        return
    }

    lobbyPlayerLimit = limit
    log.Printf("Lobby player limit set to: %d", lobbyPlayerLimit)
}

func readSecret(filePath string) (string, error) {
    data, err := os.ReadFile(filePath)
    if err != nil {
        return "", err
    }
    return strings.TrimSpace(string(data)), nil
}

var producer *kafka.Producer

func main() {
    user, _ := readSecret(os.Getenv("POSTGRES_USER_FILE"))
    password, _ := readSecret(os.Getenv("POSTGRES_PASSWORD_FILE"))
    conn, err := pgx.Connect(
        context.Background(),
        "postgres://"+user+":"+password+
        "@"+os.Getenv("POSTGRES_HOST")+":5432/"+os.Getenv("POSTGRES_DB"))
    if err != nil {
        log.Fatalf("Postgres connection failed: %v", err)
    }
    defer conn.Close(context.Background())

    producer, err = kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
    })
    if err != nil {
        log.Fatalf("Failed to create producer: %v", err)
    }
    defer producer.Close()

    go func() {
        for e := range producer.Events() {
            switch ev := e.(type) {
                case *kafka.Message:
                    if ev.TopicPartition.Error != nil {
                        log.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
                    }
            }
        }
    }()

    if err := loadComputerNames(); err != nil {
        log.Fatalf("failed to load computer names: %w", err)
    }

    loadLobbyPlayerLimit()

    err = checkAndCreateTables(conn)
    if err != nil {
        log.Fatalf("Failed to initialize tables: %v", err)
    }

    config := &kafka.ConfigMap{
        "bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "group.id":          "go-kafka-group",
        "auto.offset.reset":   "latest",
        "enable.auto.commit":  "false",
        "session.timeout.ms":  6000,
        "heartbeat.interval.ms": 2000,
        "max.poll.interval.ms": 300000,
    }

    consumer, err := kafka.NewConsumer(config)
    if err != nil {
        log.Fatalf("Failed to create consumer: %v", err)
    }
    defer consumer.Close()
    consumer_topic := "bot_db"
    err = consumer.SubscribeTopics([]string{consumer_topic}, nil)
    if err != nil {
        log.Fatalf("Failed to subscribe: %v", err)
    }

    log.Println("Consumer started. Waiting for messages...")

    for {
        msg, err := consumer.ReadMessage(10 * time.Second)
        if err != nil {
            if err.(kafka.Error).Code() == kafka.ErrTimedOut {
                continue
            }
            log.Printf("Consumer error: %v", err)
            continue
        }

        var kafkaMsg KafkaMessage
        if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
            log.Printf("Failed to decode message: %v", err)
            continue
        }

        switch kafkaMsg.Command {
            case "insert_player":
                var player PlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &player); err != nil {
                    log.Printf("Failed to parse player data: %v", err)
                    continue
                }
                err = handleInsertPlayer(conn, kafkaMsg.CorrelationId, player)
                if err != nil {
                    log.Printf("Failed to insert player: %v", err)
                }
            case "change_player":
                var player PlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &player); err != nil {
                    log.Printf("Failed to parse player data: %v", err)
                    continue
                }
                err = handleUpdatePlayer(conn, kafkaMsg.CorrelationId, player)
                if err != nil {
                    log.Printf("Failed to insert player: %v", err)
                }

            case "update_lang_player":
                var player PlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &player); err != nil {
                    log.Printf("Failed to parse player data: %v", err)
                    continue
                }
                err = handleUpdateLangPlayer(conn, kafkaMsg.CorrelationId, player)
                if err != nil {
                    log.Printf("Failed to update lang player: %v", err)
                }

            case "create_game":
                var game GameData
                if err := json.Unmarshal(kafkaMsg.Data, &game); err != nil {
                    log.Printf("Failed to parse game data: %v", err)
                    continue
                }
                err = handleCreateGame(conn, kafkaMsg.CorrelationId, game)
                if err != nil {
                    log.Printf("Failed to create game: %v", err)
                }

            case "update_game":
                var game GameData
                if err := json.Unmarshal(kafkaMsg.Data, &game); err != nil {
                    log.Printf("Failed to parse game data: %v", err)
                    continue
                }
                err = handleUpdateGame(conn, kafkaMsg.CorrelationId, game)
                if err != nil {
                    log.Printf("Failed to update game: %v", err)
                }

            case "create_computer":
                var computer ComputerGameData
                if err := json.Unmarshal(kafkaMsg.Data, &computer); err != nil {
                    log.Printf("Failed to parse computer data: %v", err)
                    continue
                }
                err = handleCreateComputer(conn, kafkaMsg.CorrelationId, computer)
                if err != nil {
                    log.Printf("Failed to create computer: %v", err)
                }

            case "add_player_game":
                var playerGame PlayerGameData
                if err := json.Unmarshal(kafkaMsg.Data, &playerGame); err != nil {
                    log.Printf("Failed to parse player game data: %v", err)
                    continue
                }
                err = handleAddPlayerGame(conn, kafkaMsg.CorrelationId, playerGame)
                if err != nil {
                    log.Printf("Failed to add player game: %v", err)
                }

            case "feedback":
                var feedback FeedBackData
                if err := json.Unmarshal(kafkaMsg.Data, &feedback); err != nil {
                    log.Printf("Failed to parse feedback data: %v", err)
                    continue
                }
                err = handleFeedback(conn, kafkaMsg.CorrelationId, feedback)
                if err != nil {
                    log.Printf("Failed to add feedback: %v", err)
                }

            case "get_current_game":
                var player_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &player_id); err != nil {
                    log.Printf("Failed to parse player id: %v", err)
                    continue
                }
                err = handleGetCurrentGame(conn, kafkaMsg.CorrelationId, player_id)
                if err != nil {
                    log.Printf("Failed to get current game: %v", err)
                }

            case "exit_from_game":
                var playerGame PlayerGameData
                if err := json.Unmarshal(kafkaMsg.Data, &playerGame); err != nil {
                    log.Printf("Failed to parse player game data: %v", err)
                    continue
                }
                err = handleExitFromGame(conn, kafkaMsg.CorrelationId, playerGame)
                if err != nil {
                    log.Printf("Failed to exit from game: %v", err)
                }

            case "get_game_report":
                var game_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &game_id); err != nil {
                    log.Printf("Failed to parse game id: %v", err)
                    continue
                }
                err = handleGetGameReport(conn, kafkaMsg.CorrelationId, game_id)
                if err != nil {
                    log.Printf("Failed to get game report: %v", err)
                }

            case "get_server_games":
                var server_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &server_id); err != nil {
                    log.Printf("Failed to parse server id: %v", err)
                    continue
                }
                err = handleGetServerGames(conn, kafkaMsg.CorrelationId, server_id)
                if err != nil {
                    log.Printf("Failed to send server games: %v", err)
                }

            case "add_step":
                var step GamesHistoryData
                if err := json.Unmarshal(kafkaMsg.Data, &step); err != nil {
                    log.Printf("Failed to parse game step data: %v", err)
                    continue
                }
                err = handleInsertStep(conn, kafkaMsg.CorrelationId, step)
                if err != nil {
                    log.Printf("Failed to insert step: %v", err)
                }

            case "get_game_names":
                var gameId int64
                if err := json.Unmarshal(kafkaMsg.Data, &gameId); err != nil {
                    log.Printf("Failed to parse game id: %v", err)
                    continue
                }
                err = handleGetGameNames(conn, kafkaMsg.CorrelationId, gameId)
                if err != nil {
                    log.Printf("Failed to get game names: %v", err)
                }

            case "get_current_players":
                var gameId int64
                if err := json.Unmarshal(kafkaMsg.Data, &gameId); err != nil {
                    log.Printf("Failed to parse game id: %v", err)
                    continue
                }
                err = handleGetCurrentPlayers(conn, kafkaMsg.CorrelationId, gameId)
                if err != nil {
                    log.Printf("Failed to get current players: %v", err)
                }

            case "create_lobby":
                var lobby LobbyData
                if err := json.Unmarshal(kafkaMsg.Data, &lobby); err != nil {
                    log.Printf("Failed to parse lobby data: %v", err)
                    continue
                }
                err = handleCreateLobby(conn, kafkaMsg.CorrelationId, lobby)

            case "join_lobby":
                var joinData struct {
                    LobbyId   int64  `json:"lobby_id"`
                    PlayerId  int64  `json:"player_id"`
                    Password  string `json:"password,omitempty"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &joinData); err != nil {
                    log.Printf("Failed to parse join lobby data: %v", err)
                    continue
                }
                err = handleJoinLobby(conn, kafkaMsg.CorrelationId, joinData)

            case "leave_lobby":
                var lobbyPlayerData LobbyPlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &lobbyPlayerData); err != nil {
                    log.Printf("Failed to parse leave lobby data: %v", err)
                    continue
                }
                err = handleLeaveLobby(conn, kafkaMsg.CorrelationId, lobbyPlayerData)

            case "set_player_ready":
                var lobbyPlayerData LobbyPlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &lobbyPlayerData); err != nil {
                    log.Printf("Failed to parse ready data: %v", err)
                    continue
                }
                err = handleSetPlayerReady(conn, kafkaMsg.CorrelationId, lobbyPlayerData)

            case "start_lobby_game":
                var lobbyData LobbyData
                if err := json.Unmarshal(kafkaMsg.Data, &lobbyData); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handleStartLobbyGame(conn, kafkaMsg.CorrelationId, lobbyData)

            case "get_random_lobby_id":
                var inputData struct {
                    ServerId   int64  `json:"server_id"`
                    PlayerId  int64  `json:"player_id"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &inputData); err != nil {
                    log.Printf("Failed to parse server id: %v", err)
                    continue
                }
                err = handleGetRandomLobbyId(conn, kafkaMsg.CorrelationId, inputData)

            case "get_lobby_id":
                var player_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &player_id); err != nil {
                    log.Printf("Failed to parse get lobby data: %v", err)
                    continue
                }
                err = handleGetLobbyId(conn, kafkaMsg.CorrelationId, player_id)

            case "get_lobby_players":
                var lobby_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &lobby_id); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handleGetLobbyPlayers(conn, kafkaMsg.CorrelationId, lobby_id)

            case "get_lobby_names":
                var lobby_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &lobby_id); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handleGetLobbyNames(conn, kafkaMsg.CorrelationId, lobby_id)

            case "prepare_to_start_lobby":
                var checkData struct {
                    LobbyId   int64  `json:"lobby_id"`
                    PlayerId  int64  `json:"player_id"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &checkData); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handlePrepareToStartLobby(conn, kafkaMsg.CorrelationId, checkData)

            case "is_lobby_host":
                var checkData struct {
                    LobbyId   int64  `json:"lobby_id"`
                    PlayerId  int64  `json:"player_id"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &checkData); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handleIsLobbyHost(conn, kafkaMsg.CorrelationId, checkData)

            case "ban_player":
                var banData struct {
                    LobbyId   int64  `json:"lobby_id"`
                    HostId    int64  `json:"host_id"`
                    PlayerId  int64  `json:"player_id"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &banData); err != nil {
                    log.Printf("Failed to parse ban player data: %v", err)
                    continue
                }
                err = handleBanPlayer(conn, kafkaMsg.CorrelationId, banData)

            case "unban_player":
                var unbanData struct {
                    LobbyId  int64 `json:"lobby_id"`
                    HostId    int64  `json:"host_id"`
                    PlayerId int64 `json:"player_id"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &unbanData); err != nil {
                    log.Printf("Failed to parse remove from blacklist data: %v", err)
                    continue
                }
                err = handleUnbanPlayer(conn, kafkaMsg.CorrelationId, unbanData)

            case "get_blacklist":
                var inputData struct {
                    LobbyId  int64 `json:"lobby_id"`
                    HostId    int64  `json:"host_id"`
                }
                if err := json.Unmarshal(kafkaMsg.Data, &inputData); err != nil {
                    log.Printf("Failed to parse get blacklist data: %v", err)
                    continue
                }
                err = handleGetBlacklist(conn, kafkaMsg.CorrelationId, inputData)

            case "server_info":
                err = handleServerInfo(kafkaMsg.CorrelationId)
                if err != nil {
                    log.Printf("Failed to handle server info request: %v", err)
                }

  // another commands

            default:
                log.Printf("Unknown command: %s", kafkaMsg.Command)
        }
    }
}

func handleInsertPlayer(conn *pgx.Conn, correlation_id string, player PlayerData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    _, err := conn.Exec(context.Background(),
        `INSERT INTO players(id, firstname, lastname, fullname, username, lang, state)
        VALUES($1, $2, $3, $4, $5, $6, $7)`,
        player.ID, player.FirstName, player.LastName, player.FullName,
        player.UserName, player.Lang, player.State)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to insert player: %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Inserted new player with id %d", player.ID)
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleUpdatePlayer(conn *pgx.Conn, correlation_id string, player PlayerData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    query := "UPDATE players SET"
    args := make([]interface{}, 0)
    argCounter := 1

    if player.FirstName != "" {
        query += fmt.Sprintf(" firstname = $%d,", argCounter)
        args = append(args, player.FirstName)
        argCounter++
    }
    if player.LastName != "" {
        query += fmt.Sprintf(" lastname = $%d,", argCounter)
        args = append(args, player.LastName)
        argCounter++
    }
    if player.FullName != "" {
        query += fmt.Sprintf(" fullname = $%d,", argCounter)
        args = append(args, player.FullName)
        argCounter++
    }
    if player.UserName != "" {
        query += fmt.Sprintf(" username = $%d,", argCounter)
        args = append(args, player.UserName)
        argCounter++
    }
    if player.State != "" {
        query += fmt.Sprintf(" state = $%d,", argCounter)
        args = append(args, player.State)
        argCounter++
    }

    if len(args) > 0 {
        query = query[:len(query)-1]
    } else {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("No fields to update for player with id %v", player.ID)
        return sendGenericResponse(response)
    }

    query += fmt.Sprintf(" WHERE id = $%d", argCounter)
    args = append(args, player.ID)

    cmdTag, err := conn.Exec(context.Background(), query, args...)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to update player: %v", err)
        return sendGenericResponse(response)
    }

    if cmdTag.RowsAffected() == 0 {
        handleInsertPlayer(conn, correlation_id, player)
    } else {
        log.Printf("Updated player with id %d", player.ID)
        response.Answer = "OK"
        return sendGenericResponse(response)
    }

    return nil
}

func handleUpdateLangPlayer(conn *pgx.Conn, correlation_id string, player PlayerData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    _, err := conn.Exec(context.Background(),
        `UPDATE players SET
            lang = $2
        WHERE id = $1`,
        player.ID, player.Lang)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to update lang player %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Updated lang player with id %d", player.ID)
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleCreateGame(conn *pgx.Conn, correlation_id string, game GameData) error {
    var newID int64
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    err = tx.QueryRow(context.Background(), "SELECT nextval('games_id_seq')").Scan(&newID)
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to generate new ID: %v", err)
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
        `INSERT INTO games(id, server_id, mode, stage, step, secret_value)
        VALUES($1, $2, $3, $4, $5, $6)`,
        newID, game.ServerId, game.Mode, game.Stage, game.Step, game.SecretValue)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to insert game: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }
    log.Printf("Inserted game with id %d", newID)

    data := struct {
        Table string `json:"table"`
        ID    int64  `json:"id"`
    }{
        Table: "games",
        ID:    newID,
    }

    response.Answer = "OK"
    response.Data = data
    return sendGenericResponse(response)
}

func handleExitFromGame(conn *pgx.Conn, correlation_id string, playerGame PlayerGameData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    _, err := conn.Exec(context.Background(),
        `UPDATE player_games
            SET is_current_game = false
            WHERE player_id = $1 AND game_id = $2`,
        playerGame.PlayerId, playerGame.GameId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to exit player from game: %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Player %d exited from game %d", playerGame.PlayerId, playerGame.GameId)
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleUpdateGame(conn *pgx.Conn, correlation_id string, game GameData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    if game.SecretValue == 0 {
        _, err = tx.Exec(context.Background(),
            `UPDATE games SET
            server_id = $2,
            stage = $3,
            step = $4
            WHERE id = $1`,
            game.ID, game.ServerId, game.Stage, game.Step)
    } else {
        _, err = tx.Exec(context.Background(),
            `UPDATE games SET
            server_id = $2,
            stage = $3,
            step = $4,
            secret_value = $5
            WHERE id = $1`,
            game.ID, game.ServerId, game.Stage, game.Step, game.SecretValue)
    }
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to update game: %v", err)
        return sendGenericResponse(response)
    }

    if game.Stage == "FINISHED" {
        var gameMode string
        err = tx.QueryRow(context.Background(),
            `SELECT mode FROM games WHERE id = $1`,
            game.ID).Scan(&gameMode)
        if err != nil {
            tx.Rollback(context.Background())
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to query game mode: %v", err)
            return sendGenericResponse(response)
        }

        if gameMode == "lobby" {
            _, err = tx.Exec(context.Background(),
                `UPDATE lobbies SET status = 'FINISHED' WHERE game_id = $1`,
                game.ID)
            if err != nil {
                tx.Rollback(context.Background())
                response.Answer = "Error"
                response.Error = fmt.Sprintf("failed to update lobby status: %v", err)
                return sendGenericResponse(response)
            }
            log.Printf("Updated lobby status to FINISHED for game %d", game.ID)

            _, err = tx.Exec(context.Background(),
            `UPDATE lobby_players SET is_ready = false, in_lobby = false
                WHERE lobby_id = (SELECT id FROM lobbies WHERE game_id = $1)`,
            game.ID)
            if err != nil {
                tx.Rollback(context.Background())
                response.Answer = "Error"
                response.Error = fmt.Sprintf("failed to reset player ready states and in_lobby: %v", err)
                return sendGenericResponse(response)
            }
            log.Printf("Reset all player ready states to false and in_lobby to false for lobby with game %d", game.ID)
        }
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Updated game with id %d", game.ID)
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleCreateComputer(conn *pgx.Conn, correlation_id string, computer ComputerGameData) error {
    var newID int64
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    err = tx.QueryRow(context.Background(), "SELECT nextval('computers_id_seq')").Scan(&newID)
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to generate new ID: %v", err)
        return sendGenericResponse(response)
    }

    names, ok := computerNames[computer.GameBrain]
    if !ok {
        names = []string{"Unknown"}
    }
    rand.Seed(time.Now().UnixNano())

    rand_i := rand.Intn(len(names))
    computer.Name = names[rand_i]

    _, err = tx.Exec(context.Background(),
        `INSERT INTO computers(computer_id, player_id, server_id, game_id, game_brain, name)
        VALUES($1, $2, $3, $4, $5, $6)`,
        newID, computer.PlayerId, computer.ServerId, computer.GameId, computer.GameBrain, computer.Name)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to insert computer: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }
    log.Printf("Inserted computer with id %d and name %s", newID, computer.Name)

    data := struct {
        Table string `json:"table"`
        ID    int64  `json:"id"`
    }{
        Table: "computers",
        ID:    newID,
    }
    response.Answer = "OK"
    response.Data = data
    return sendGenericResponse(response)
}

func handleAddPlayerGame(conn *pgx.Conn, correlation_id string, playerGame PlayerGameData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
        `UPDATE player_games
            SET is_current_game = false
            WHERE player_id = $1`,
        playerGame.PlayerId)
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to reset current games: %v", err)
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
    `INSERT INTO player_games(player_id, server_id, game_id, is_current_game, is_host)
    VALUES($1, $2, $3, $4, $5)`,
    playerGame.PlayerId, playerGame.ServerId, playerGame.GameId, playerGame.IsCurrentGame, playerGame.IsHost)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to insert player game: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Inserted player game")
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleFeedback(conn *pgx.Conn, correlation_id string, feedback FeedBackData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    _, err := conn.Exec(context.Background(),
        `INSERT INTO feedback(username, message)
        VALUES($1, $2)`,
        feedback.UserName, feedback.Message)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to add feedback: %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Added feedback")
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleGetCurrentGame(conn *pgx.Conn, correlation_id string, player_id int64) error {
    var gameID int64
    var stage string
    var finished bool
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    err := conn.QueryRow(context.Background(),
    `SELECT g.id, g.stage
        FROM games g
        JOIN player_games pg ON g.id = pg.game_id
        WHERE pg.player_id = $1 AND pg.is_current_game = true`,
    player_id).Scan(&gameID, &stage)

    if err != nil {
        if err == pgx.ErrNoRows {
            gameID = 0
            finished = false
        } else {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to query current game: %v", err)
            return sendGenericResponse(response)
        }
    } else {
        finished = (stage == "FINISHED")
    }

    data := struct {
        ID            int64  `json:"id"`
        Finished      bool   `json:"finished"`
    }{
        ID: gameID,
        Finished: finished,
    }

    response.Answer = "OK"
    response.Data = data
    log.Printf("Sent current game ID %d for player %d (finished: %v)", gameID, player_id, finished)
    return sendGenericResponse(response)
}

func handleGetServerGames(conn *pgx.Conn, correlation_id string, server_id int64) error {
    games := make([]GameData, 0)
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }
    defer tx.Rollback(context.Background())

    rows, err := tx.Query(context.Background(),
        `SELECT id, server_id, mode, stage, step, secret_value
         FROM games
         WHERE server_id = $1 AND stage != 'FINISHED'`,
        server_id)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query server games: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var game GameData
        if err := rows.Scan(&game.ID, &game.ServerId, &game.Mode, &game.Stage, &game.Step, &game.SecretValue); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan game row: %v", err)
            return sendGenericResponse(response)
        }
        games = append(games, game)
    }

    players := make([]PlayerData, 0)
    rows, err = tx.Query(context.Background(),
        `SELECT id, firstname, lastname, fullname, username, lang, state
         FROM players`)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query players: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var pg PlayerData
        if err := rows.Scan(&pg.ID, &pg.FirstName, &pg.LastName, &pg.FullName, &pg.UserName, &pg.Lang, &pg.State); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan players row: %v", err)
            return sendGenericResponse(response)
        }
        players = append(players, pg)
    }


    playerGames := make([]PlayerGameData, 0)
    rows, err = tx.Query(context.Background(),
        `SELECT player_id, server_id, game_id, is_current_game, is_host
         FROM player_games
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )`, server_id)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query player_games: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var pg PlayerGameData
        if err := rows.Scan(&pg.PlayerId, &pg.ServerId, &pg.GameId, &pg.IsCurrentGame, &pg.IsHost); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan player_games row: %v", err)
            return sendGenericResponse(response)
        }
        playerGames = append(playerGames, pg)
    }

    computerGames := make([]ComputerGameData, 0)
    rows, err = tx.Query(context.Background(),
        `SELECT computer_id, player_id, server_id, game_id, game_brain, name
         FROM computers
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )`, server_id)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query computers: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var cg ComputerGameData
        if err := rows.Scan(&cg.ComputerId, &cg.PlayerId, &cg.ServerId, &cg.GameId, &cg.GameBrain, &cg.Name); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan computer row: %v", err)
            return sendGenericResponse(response)
        }
        computerGames = append(computerGames, cg)
    }

    history := make([]GamesHistoryData, 0)
    rows, err = tx.Query(context.Background(),
        `SELECT game_id, player_id, server_id, step, game_value, bulls, cows, is_computer, is_give_up, timestamp
         FROM games_history
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )
         ORDER BY timestamp ASC`, server_id)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query games history: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var h GamesHistoryData
        if err := rows.Scan(&h.GameId, &h.PlayerId, &h.ServerId, &h.Step, &h.GameValue, &h.Bulls, &h.Cows, &h.IsComputer, &h.IsGiveUp, &h.Timestamp); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan history row: %v", err)
            return sendGenericResponse(response)
        }
        history = append(history, h)
    }

    tx.Rollback(context.Background())

    data := struct {
        Games         []GameData         `json:"games"`
        Players       []PlayerData       `json:"players"`
        PlayerGames   []PlayerGameData   `json:"player_games"`
        ComputerGames []ComputerGameData `json:"computer_games"`
        History       []GamesHistoryData `json:"history"`
    }{
        Games:         games,
        Players:       players,
        PlayerGames:   playerGames,
        ComputerGames: computerGames,
        History:       history,
    }
    response.Answer = "OK"
    response.Data = data
    log.Printf("Sent server games data for server_id %d (with computers and history)", server_id)
    return sendGenericResponse(response)
}

func handleGetGameNames(conn *pgx.Conn, correlation_id string, gameId int64) error {
    var names []NameData
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    rows, err := conn.Query(context.Background(),
        `SELECT p.id, p.username
         FROM players p
         JOIN player_games pg ON p.id = pg.player_id
         WHERE pg.game_id = $1`, gameId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query players: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var username string
        if err := rows.Scan(&id, &username); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan player row: %v", err)
            return sendGenericResponse(response)
        }
        names = append(names, NameData{
            ID:       id,
            IsPlayer: true,
            Name:     username,
        })
    }

    rows, err = conn.Query(context.Background(),
        `SELECT computer_id, name
         FROM computers
         WHERE game_id = $1`, gameId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query computers: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan computer row: %v", err)
            return sendGenericResponse(response)
        }
        names = append(names, NameData{
            ID:       id,
            IsPlayer: false,
            Name:     name,
        })
    }

    data := struct {
    Names         []NameData `json:"names"`
    }{
        Names:         names,
    }
    response.Answer = "OK"
    response.Data = data

    log.Printf("Sent game names for game_id %d", gameId)
    return sendGenericResponse(response)
}

func handleGetGameReport(conn *pgx.Conn, correlation_id string, gameId int64) error {
    var steps []GamesHistoryData
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    rows, err := conn.Query(context.Background(),
        `SELECT game_id, player_id, server_id, step, game_value, bulls, cows, is_computer, is_give_up, timestamp
         FROM games_history
         WHERE game_id = $1
         ORDER BY timestamp ASC`, gameId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query game steps: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var step GamesHistoryData
        if err := rows.Scan(&step.GameId, &step.PlayerId, &step.ServerId, &step.Step, &step.GameValue, &step.Bulls, &step.Cows, &step.IsComputer, &step.IsGiveUp, &step.Timestamp); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan step row: %v", err)
            return sendGenericResponse(response)
        }
        steps = append(steps, step)
    }

    data := struct {
        Steps  []GamesHistoryData `json:"steps"`
    }{
        Steps:  steps,
    }

    response.Answer = "OK"
    response.Data = data
    return sendGenericResponse(response)
}

func handleCreateLobby(conn *pgx.Conn, correlation_id string, lobby LobbyData) error {
    var newID int64
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    err = tx.QueryRow(context.Background(), "SELECT nextval('lobbies_id_seq')").Scan(&newID)
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to generate new ID: %v", err)
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
        `INSERT INTO lobbies(id, server_id, host_id, game_id, is_private, password, status)
        VALUES($1, $2, $3, $4, $5, $6, $7)`,
        newID, lobby.ServerId, lobby.HostId, lobby.GameId, lobby.IsPrivate, lobby.Password, "WAITING")

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to insert lobby: %v", err)
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
        `INSERT INTO lobby_players(lobby_id, player_id, is_ready, joined_at, host, access, in_lobby)
        VALUES($1, $2, $3, $4, $5, $6, $7)`,
        newID, lobby.HostId, false, time.Now(), true, true, true)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to add creator to lobby: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        Table string `json:"table"`
        ID    int64  `json:"id"`
    }{
        Table: "lobbies",
        ID:    newID,
    }

    response.Answer = "OK"
    response.Data = data
    log.Printf("Inserted lobby with id %d", newID)
    return sendGenericResponse(response)
}

func handleJoinLobby(conn *pgx.Conn, correlation_id string, joinData struct {
    LobbyId   int64  `json:"lobby_id"`
    PlayerId  int64  `json:"player_id"`
    Password  string `json:"password,omitempty"`
}) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    var isBlacklisted bool
    err = tx.QueryRow(context.Background(),
        `SELECT EXISTS(SELECT 1 FROM lobby_blacklist WHERE lobby_id = $1 AND player_id = $2)`,
        joinData.LobbyId, joinData.PlayerId).Scan(&isBlacklisted)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to check player blacklist status: %v", err)
        return sendGenericResponse(response)
    }

    if isBlacklisted {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerPlayerIsBlacklisted"
        return sendGenericResponse(response)
    }

    var existingLobbyId int64
    err = tx.QueryRow(context.Background(),
        `SELECT lobby_id FROM lobby_players
            WHERE player_id = $1 AND access = true AND lobby_id != $2`,
        joinData.PlayerId, joinData.LobbyId).Scan(&existingLobbyId)

    if err == nil {

        data := struct {
        ID    int64  `json:"id"`
        }{
            ID:    existingLobbyId,
        }
        response.Data = data
        response.Answer = "Warning"
        response.Error = "DBAnswerAlreadyInLobby"
        return sendGenericResponse(response)
    } else if err != pgx.ErrNoRows {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to check player lobby status: %v", err)
        return sendGenericResponse(response)
    }

    var lobby LobbyData
    err = tx.QueryRow(context.Background(),
        `SELECT id, server_id, host_id, game_id, is_private, password, status
            FROM lobbies WHERE id = $1`,
        joinData.LobbyId).Scan(
            &lobby.ID, &lobby.ServerId, &lobby.HostId, &lobby.GameId, &lobby.IsPrivate,
            &lobby.Password, &lobby.Status)

    if err != nil {

        tx.Rollback(context.Background())
        if err == pgx.ErrNoRows {
        response.Answer = "Warning"
        response.Error = "DBAnswerLobbyNotFound"
            return sendGenericResponse(response)
        }
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby: %v", err)
        return sendGenericResponse(response)
    }

    hasAccess := true
    if lobby.IsPrivate {
        if lobby.Password != joinData.Password {
            if joinData.Password == "" {
                hasAccess = false
            } else {
                tx.Rollback(context.Background())
                response.Answer = "Warning"
                response.Error = "DBAnswerInvalidPassword"
                return sendGenericResponse(response)
            }
        }
    }

    if lobby.Status == "STARTED" {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerLobbyIsntAcceptingPlayers"
        return sendGenericResponse(response)
    }

    var playerCount int
    err = tx.QueryRow(context.Background(),
        `SELECT COUNT(*) FROM lobby_players WHERE lobby_id = $1 AND access = true`,
        joinData.LobbyId).Scan(&playerCount)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to check lobby player count: %v", err)
        return sendGenericResponse(response)
    }

    var existingPlayer int64
    var existingAccess bool
    err = tx.QueryRow(context.Background(),
        `SELECT player_id, access FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        joinData.LobbyId, joinData.PlayerId).Scan(&existingPlayer, &existingAccess)

    if err == nil {
        if !hasAccess {
                if err := tx.Commit(context.Background()); err != nil {
                    response.Answer = "Error"
                    response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
                    return sendGenericResponse(response)
                }
                response.Answer = "Warning"
                response.Error = "DBAnswerPasswordNeeded"
                return sendGenericResponse(response)
        } else if !existingAccess {
            if playerCount >= lobbyPlayerLimit {
                tx.Rollback(context.Background())
                response.Answer = "Warning"
                response.Error = "DBAnswerLobbyIsFull"
                return sendGenericResponse(response)
            }
            _, err = tx.Exec(context.Background(),
                `UPDATE lobby_players SET access = true, in_lobby = true WHERE lobby_id = $1 AND player_id = $2`,
                joinData.LobbyId, joinData.PlayerId)
            if err != nil {
                tx.Rollback(context.Background())
                response.Answer = "Error"
                response.Error = fmt.Sprintf("failed to update player access: %v", err)
                return sendGenericResponse(response)
            }
            log.Printf("Updated player %d access to true and in_lobby to true in lobby %d", joinData.PlayerId, joinData.LobbyId)
        } else {
            _, err = tx.Exec(context.Background(),
                `UPDATE lobby_players SET in_lobby = true WHERE lobby_id = $1 AND player_id = $2`,
                joinData.LobbyId, joinData.PlayerId)
            if err != nil {
                tx.Rollback(context.Background())
                response.Answer = "Error"
                response.Error = fmt.Sprintf("failed to update player in_lobby: %v", err)
                return sendGenericResponse(response)
            }
            log.Printf("Updated player %d in_lobby to true in lobby %d", joinData.PlayerId, joinData.LobbyId)
        }
    } else {
        if err == pgx.ErrNoRows {
            if hasAccess && playerCount >= lobbyPlayerLimit {
                tx.Rollback(context.Background())
                response.Answer = "Warning"
                response.Error = "DBAnswerLobbyIsFull"
                return sendGenericResponse(response)
            }
            _, err = tx.Exec(context.Background(),
                `INSERT INTO lobby_players(lobby_id, player_id, is_ready, joined_at, host, access, in_lobby)
                VALUES($1, $2, $3, $4, $5, $6, $7)`,
                joinData.LobbyId, joinData.PlayerId, false, time.Now(), false, hasAccess, hasAccess)

            if err != nil {
                tx.Rollback(context.Background())
                response.Answer = "Error"
                response.Error = fmt.Sprintf("failed to add player to lobby: %v", err)
                return sendGenericResponse(response)
            }
            if !hasAccess {
                if err := tx.Commit(context.Background()); err != nil {
                    response.Answer = "Error"
                    response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
                    return sendGenericResponse(response)
                }
                response.Answer = "Warning"
                response.Error = "DBAnswerPasswordNeeded"
                return sendGenericResponse(response)
            }

        } else {
            tx.Rollback(context.Background())
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to check player existence: %v", err)
            return sendGenericResponse(response)
        }
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        ID    int64  `json:"id"`
    }{
        ID:    lobby.GameId,
    }

    response.Answer = "OK"
    response.Data = data
    log.Printf("Player %d joined lobby %d with game_id %d", joinData.PlayerId, joinData.LobbyId, lobby.GameId)
    return sendGenericResponse(response)
}

func handleLeaveLobby(conn *pgx.Conn, correlation_id string, lobbyPlayerData LobbyPlayerData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }
    defer tx.Rollback(context.Background())

    var hostId int64
    var isHost bool
    err = tx.QueryRow(context.Background(),
        `SELECT host_id FROM lobbies WHERE id = $1`,
        lobbyPlayerData.LobbyId).Scan(&hostId)

    isHost = hostId == lobbyPlayerData.PlayerId

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to get lobby info: %v", err)
        return sendGenericResponse(response)
    }

    result, err := tx.Exec(context.Background(),
        `DELETE FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        lobbyPlayerData.LobbyId, lobbyPlayerData.PlayerId)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to remove player from lobby: %v", err)
        return sendGenericResponse(response)
    }

    if result.RowsAffected() == 0 {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("Player %d not found in lobby %d", lobbyPlayerData.PlayerId, lobbyPlayerData.LobbyId)
        return sendGenericResponse(response)
    }

    var playerCount int
    err = tx.QueryRow(context.Background(),
        `SELECT COUNT(*) FROM lobby_players WHERE lobby_id = $1`,
        lobbyPlayerData.LobbyId).Scan(&playerCount)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to check player count: %v", err)
        return sendGenericResponse(response)
    }

    if playerCount == 0 {
        _, err = tx.Exec(context.Background(),
            `DELETE FROM lobbies WHERE id = $1`,
            lobbyPlayerData.LobbyId)
        if err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to delete empty lobby: %v", err)
            return sendGenericResponse(response)
        }
        log.Printf("Deleted empty lobby %d", lobbyPlayerData.LobbyId)
    } else {
        if isHost {
            var newHostId int64
            err = tx.QueryRow(context.Background(),
                `SELECT player_id FROM lobby_players
                WHERE lobby_id = $1 AND player_id != $2
                ORDER BY joined_at ASC
                LIMIT 1`,
                lobbyPlayerData.LobbyId, lobbyPlayerData.PlayerId).Scan(&newHostId)

            if err != nil {
                if err == pgx.ErrNoRows {
                    log.Printf("No other players in lobby %d to assign as host", lobbyPlayerData.LobbyId)
                } else {
                    response.Answer = "Error"
                    response.Error = fmt.Sprintf("failed to find new host: %v", err)
                    return sendGenericResponse(response)
                }
            } else {
                _, err = tx.Exec(context.Background(),
                    `UPDATE lobbies SET host_id = $1 WHERE id = $2`,
                    newHostId, lobbyPlayerData.LobbyId)
                if err != nil {
                    response.Answer = "Error"
                    response.Error = fmt.Sprintf("failed to update lobby host: %v", err)
                    return sendGenericResponse(response)
                }

                _, err := tx.Exec(context.Background(),
                    `UPDATE lobby_players SET host = true WHERE lobby_id = $1 AND player_id = $2`,
                lobbyPlayerData.LobbyId, newHostId)
                if err != nil {
                    response.Answer = "Error"
                    response.Error = fmt.Sprintf("failed to update lobby player host: %v", err)
                    return sendGenericResponse(response)
                }
                log.Printf("Transferred host from player %d to player %d in lobby %d",
                    lobbyPlayerData.PlayerId, newHostId, lobbyPlayerData.LobbyId)
            }
        }
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        IsHost    bool  `json:"is_host"`
    }{
        IsHost:    isHost,
    }

    response.Answer = "OK"
    response.Data = data
    log.Printf("Player %d left lobby %d", lobbyPlayerData.PlayerId, lobbyPlayerData.LobbyId)
    return sendGenericResponse(response)
}

func handleIsLobbyHost(conn *pgx.Conn, correlation_id string, checkData struct {
    LobbyId   int64  `json:"lobby_id"`
    PlayerId  int64  `json:"player_id"`
}) error {
    var isHost bool
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    err := conn.QueryRow(context.Background(),
        `SELECT host
         FROM lobby_players
         WHERE lobby_id = $1
         AND player_id = $2`, checkData.LobbyId, checkData.PlayerId).Scan(&isHost)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby host: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        IsHost bool `json:"is_host"`
    }{
        IsHost: isHost,
    }

    response.Answer = "OK"
    response.Data = data
    return sendGenericResponse(response)
}

func handleBanPlayer(conn *pgx.Conn, correlation_id string, banData struct {
    LobbyId   int64  `json:"lobby_id"`
    HostId    int64  `json:"host_id"`
    PlayerId  int64  `json:"player_id"`
}) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    var isHost bool
    err = tx.QueryRow(context.Background(),
        `SELECT host FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        banData.LobbyId, banData.HostId).Scan(&isHost)

    if err != nil {
        tx.Rollback(context.Background())
        if err == pgx.ErrNoRows {
            response.Answer = "Warning"
            response.Error = "DBAnswerPlayerNotInLobby"
            return sendGenericResponse(response)
        }
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby host: %v", err)
        return sendGenericResponse(response)
    }

    if !isHost {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerOnlyHostCanBanPlayers"
        return sendGenericResponse(response)
    }

    var playerExists bool
    err = tx.QueryRow(context.Background(),
        `SELECT EXISTS(SELECT 1 FROM lobby_players WHERE lobby_id = $1 AND player_id = $2)`,
        banData.LobbyId, banData.PlayerId).Scan(&playerExists)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to check player existence: %v", err)
        return sendGenericResponse(response)
    }

    if !playerExists {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerPlayerNotInLobby"
        return sendGenericResponse(response)
    }

    if banData.HostId == banData.PlayerId {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerCannotBanHost"
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
        `INSERT INTO lobby_blacklist(lobby_id, player_id, banned_by_id, reason, banned_at)
        VALUES($1, $2, $3, $4, $5)`,
        banData.LobbyId, banData.PlayerId, banData.HostId, "Banned by host", time.Now())

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to add player to blacklist: %v", err)
        return sendGenericResponse(response)
    }

    _, err = tx.Exec(context.Background(),
        `DELETE FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        banData.LobbyId, banData.PlayerId)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to remove player from lobby: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        IsBan bool `json:"is_ban"`
    }{
        IsBan: true,
    }

    response.Answer = "OK"
    response.Data = data
    return sendGenericResponse(response)
}

func handleSetPlayerReady(conn *pgx.Conn, correlation_id string, lobbyPlayerData LobbyPlayerData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    result, err := conn.Exec(context.Background(),
        `UPDATE lobby_players SET is_ready = $1
         WHERE lobby_id = $2 AND player_id = $3`,
        lobbyPlayerData.IsReady, lobbyPlayerData.LobbyId, lobbyPlayerData.PlayerId)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to update player readiness: %v", err)
        return sendGenericResponse(response)
    }

    if result.RowsAffected() == 0 {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("player %d not found in lobby %d", lobbyPlayerData.PlayerId, lobbyPlayerData.LobbyId)
        return sendGenericResponse(response)
    }

    log.Printf("Player %d readiness set to %t in lobby %d",
        lobbyPlayerData.PlayerId, lobbyPlayerData.IsReady, lobbyPlayerData.LobbyId)
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleStartLobbyGame(conn *pgx.Conn, correlation_id string, lobbyDataIn LobbyData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }
    var lobby LobbyData

    err = tx.QueryRow(context.Background(),
        `SELECT id, server_id, host_id, game_id, is_private, status
         FROM lobbies WHERE id = $1`,
        lobbyDataIn.ID).Scan(
            &lobby.ID, &lobby.ServerId, &lobby.HostId, &lobby.GameId, &lobby.IsPrivate, &lobby.Status)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby: %v", err)
        return sendGenericResponse(response)
    }

    if lobby.Status == "STARTED" {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerLobbyCannotStartGameInCurrentStatus"
        return sendGenericResponse(response)
    }

    rows, err := tx.Query(context.Background(),
        `SELECT player_id, is_ready FROM lobby_players WHERE lobby_id = $1 AND access = true`,
        lobbyDataIn.ID)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby players: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    players := make([]struct {
        PlayerId int64
        IsReady  bool
    }, 0)

    for rows.Next() {
        var player struct {
            PlayerId int64
            IsReady  bool
        }
        if err := rows.Scan(&player.PlayerId, &player.IsReady); err != nil {
            tx.Rollback(context.Background())
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan player: %v", err)
            return sendGenericResponse(response)
        }
        players = append(players, player)
    }

    for _, player := range players {
        if !player.IsReady {
            tx.Rollback(context.Background())
            response.Answer = "Warning"
            response.Error = "DBAnswerNotAllPlayersAreReady"
            return sendGenericResponse(response)
        }
    }

    _, err = tx.Exec(context.Background(),
        `UPDATE lobbies SET status = 'STARTED', game_id = $1 WHERE id = $2`,
        lobbyDataIn.GameId, lobbyDataIn.ID)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to update lobby status: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        ID    int64  `json:"id"`
    }{
        ID:    lobbyDataIn.GameId,
    }

    response.Answer = "OK"
    response.Data = data
    log.Printf("Host player %d started game %d from lobby %d with %d players",
        lobby.HostId, lobby.GameId, lobby.ID, len(players))
    return sendGenericResponse(response)
}

func checkAndCreateTables(conn *pgx.Conn) error {
    var playersTableExists bool
    err := conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'players')").Scan(&playersTableExists)
    if err != nil {
    return err
    }

    if !playersTableExists {
    _, err := conn.Exec(context.Background(),
        `CREATE TABLE players (
        id BIGINT PRIMARY KEY,
        firstname TEXT,
        lastname TEXT,
        fullname TEXT,
        username TEXT,
        lang TEXT,
        state TEXT
        )`)
    if err != nil {
        return err
    }
    log.Println("Table 'players' created successfully")
    }

    var gamesTableExists bool
    err = conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'games')").Scan(&gamesTableExists)
    if err != nil {
        return err
    }

    if !gamesTableExists {
    _, err := conn.Exec(context.Background(),
        `CREATE TABLE games (
        id BIGINT PRIMARY KEY,
        mode TEXT,
        server_id BIGINT,
        stage TEXT,
        step INTEGER,
        secret_value INTEGER
        );
        CREATE SEQUENCE games_id_seq START 1;`)
    if err != nil {
    return err
    }
    log.Println("Table 'games' and sequence 'games_id_seq' created successfully")
    }

    var playerGamesTableExists bool
    err = conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'player_games')").Scan(&playerGamesTableExists)
    if err != nil {
    return err
    }

    if !playerGamesTableExists {
        _, err := conn.Exec(context.Background(),
        `CREATE TABLE player_games (
        player_id BIGINT,
        server_id BIGINT,
        game_id BIGINT,
        is_current_game BOOLEAN,
        is_host BOOLEAN
        )`)
        if err != nil {
        return err
        }
        log.Println("Table 'player_games' created successfully")
    }

    var computerGamesTableExists bool
    err = conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'computers')").Scan(&computerGamesTableExists)
    if err != nil {
    return err
    }

    if !computerGamesTableExists {
        _, err := conn.Exec(context.Background(),
            `CREATE TABLE computers (
                computer_id BIGINT,
                player_id BIGINT,
                server_id BIGINT,
                game_id BIGINT,
                game_brain TEXT,
                name TEXT
            );
            CREATE SEQUENCE computers_id_seq START 1;`)
        if err != nil {
            return err
        }
        log.Println("Table 'computers' and sequence 'computers_id_seq' created successfully")
    }

    var gamesHistoryTableExists bool
    err = conn.QueryRow(context.Background(),
    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'games_history')").Scan(&gamesHistoryTableExists)
    if err != nil {
    return err
    }

    if !gamesHistoryTableExists {
    _, err := conn.Exec(context.Background(),
        `CREATE TABLE games_history (
        game_id BIGINT,
        player_id BIGINT,
        server_id BIGINT,
        step INTEGER,
        game_value INTEGER,
        bulls INTEGER,
        cows INTEGER,
        is_computer BOOLEAN,
        is_give_up BOOLEAN,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`)
    if err != nil {
        return err
    }
    log.Println("Table 'games_history' created successfully")
    }

    var feedBackTableExists bool
    err = conn.QueryRow(context.Background(),
    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'feedback')").Scan(&feedBackTableExists)
    if err != nil {
    return err
    }

    if !feedBackTableExists {
    _, err := conn.Exec(context.Background(),
        `CREATE TABLE feedback (
        username TEXT,
        message TEXT
        )`)
    if err != nil {
        return err
    }
    log.Println("Table 'feedback' created successfully")
}

    var lobbiesTableExists bool
    err = conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'lobbies')").Scan(&lobbiesTableExists)
    if err != nil {
        return err
    }

    if !lobbiesTableExists {
        _, err := conn.Exec(context.Background(),
            `CREATE TABLE lobbies (
                id BIGINT PRIMARY KEY,
                server_id BIGINT,
                host_id BIGINT,
                game_id BIGINT,
                is_private BOOLEAN,
                password TEXT,
                status TEXT
            );
            CREATE SEQUENCE lobbies_id_seq START 1;`)
        if err != nil {
            return err
        }
        log.Println("Table 'lobbies' and sequence 'lobbies_id_seq' created successfully")
    }

    var lobbyPlayersTableExists bool
    err = conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'lobby_players')").Scan(&lobbyPlayersTableExists)
    if err != nil {
        return err
    }

    if !lobbyPlayersTableExists {
        _, err := conn.Exec(context.Background(),
            `CREATE TABLE lobby_players (
                lobby_id BIGINT,
                player_id BIGINT,
                is_ready BOOLEAN,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                host BOOLEAN,
                access BOOLEAN DEFAULT false,
                in_lobby BOOLEAN DEFAULT false
            )`)
        if err != nil {
            return err
        }
        log.Println("Table 'lobby_players' created successfully")
    }

    var blacklistTableExists bool
    err = conn.QueryRow(context.Background(),
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'lobby_blacklist')").Scan(&blacklistTableExists)
    if err != nil {
        return err
    }

    if !blacklistTableExists {
        _, err := conn.Exec(context.Background(),
            `CREATE TABLE lobby_blacklist (
                id BIGSERIAL PRIMARY KEY,
                lobby_id BIGINT,
                player_id BIGINT,
                banned_by_id BIGINT,
                reason TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`)
        if err != nil {
            return err
        }
        log.Println("Table 'lobby_blacklist' created successfully")
    }

    return nil
}

func handleGetCurrentPlayers(conn *pgx.Conn, correlation_id string, gameId int64) error {
    var players []CurrentPlayerInfo
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    rows, err := conn.Query(context.Background(),
        `SELECT pg.player_id,
                COALESCE(bool_or(gh.is_give_up), false) as give_up,
                EXISTS (
                    SELECT 1 FROM games_history gh2
                    WHERE gh2.game_id = $1
                    AND gh2.player_id = pg.player_id
                    AND gh2.bulls = 4
                ) as finished
         FROM player_games pg
         LEFT JOIN games_history gh ON pg.game_id = gh.game_id AND pg.player_id = gh.player_id
         WHERE pg.game_id = $1 AND pg.is_current_game = true
         GROUP BY pg.player_id`,
        gameId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query current players: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var player CurrentPlayerInfo
        if err := rows.Scan(&player.PlayerId, &player.GiveUp, &player.Finished); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan player info: %v", err)
            return sendGenericResponse(response)
        }
        players = append(players, player)
    }

    data := struct {
        GameId  int64                `json:"game_id"`
        Players []CurrentPlayerInfo `json:"players"`
    }{
        GameId:  gameId,
        Players: players,
    }
    response.Answer = "OK"
    response.Data = data
    log.Printf("Sent current players for game_id %d: %v", gameId, players)
    return sendGenericResponse(response)
}

func handleInsertStep(conn *pgx.Conn, correlation_id string, step GamesHistoryData) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    _, err := conn.Exec(context.Background(),
        `INSERT INTO games_history(game_id, player_id, server_id, step, game_value, bulls, cows, is_computer, is_give_up, timestamp)
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        step.GameId, step.PlayerId, step.ServerId, step.Step, step.GameValue, step.Bulls, step.Cows, step.IsComputer, step.IsGiveUp, step.Timestamp)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to insert step: %v", err)
        return sendGenericResponse(response)
    }

    log.Printf("Inserted step with game id %d at %v", step.GameId, step.Timestamp)
    response.Answer = "OK"
    return sendGenericResponse(response)
}

func handleGetRandomLobbyId(conn *pgx.Conn, correlation_id string, inputData struct {
    ServerId   int64  `json:"server_id"`
    PlayerId  int64  `json:"player_id"`
}) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    rows, err := conn.Query(context.Background(),
        `SELECT l.id
         FROM lobbies l
         WHERE l.server_id = $1
           AND l.is_private = false
           AND l.status != 'STARTED'
           AND NOT EXISTS (
               SELECT 1 FROM lobby_blacklist lb
               WHERE lb.lobby_id = l.id AND lb.player_id = $2
           )`,
        inputData.ServerId, inputData.PlayerId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query available lobbies: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    var availableLobbyIds []int64
    for rows.Next() {
        var lobbyId int64
        if err := rows.Scan(&lobbyId); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan lobby id: %v", err)
            return sendGenericResponse(response)
        }
        availableLobbyIds = append(availableLobbyIds, lobbyId)
    }

    var randlobbyId int64
    if len(availableLobbyIds) > 0 {
        rand.Seed(time.Now().UnixNano())
        randomIndex := rand.Intn(len(availableLobbyIds))
        randlobbyId = availableLobbyIds[randomIndex]
    } else {
        randlobbyId = 0
    }

    data := struct {
        Table string `json:"table"`
        LobbyId int64 `json:"lobby_id"`
    }{
        Table:        "lobbies",
        LobbyId: randlobbyId,
    }

    response.Answer = "OK"
    response.Data = data
    log.Printf("Sent random lobby ID %d for server_id %d, player_id %d (from %d available lobbies)",
        randlobbyId, inputData.ServerId, inputData.PlayerId, len(availableLobbyIds))
    return sendGenericResponse(response)
}

func handleGetLobbyId(conn *pgx.Conn, correlation_id string, player_id int64) error {
    var lobbyId int64
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    err := conn.QueryRow(context.Background(),
        `SELECT lobby_id
         FROM lobby_players
         WHERE player_id = $1`, player_id).Scan(&lobbyId)
    if err != nil {
        response.Answer = "Warning"
        response.Error = "DBAnswerPlayerNotInLobby"
        return sendGenericResponse(response)
    }
    data := struct {
        Table string `json:"table"`
        LobbyId int64 `json:"lobby_id"`
    }{
        Table:        "lobby_players",
        LobbyId: lobbyId,
    }

    response.Answer = "OK"
    response.Data = data

    log.Printf("Sent lobby ID %d for player_id %d", lobbyId, player_id)
    return sendGenericResponse(response)
}

func handleGetLobbyPlayers(conn *pgx.Conn, correlation_id string, lobby_id int64) error {
    players := make([]LobbyPlayerData, 0)
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    rows, err := conn.Query(context.Background(),
        `SELECT lobby_id, player_id, is_ready, joined_at, host, access, in_lobby
         FROM lobby_players
         WHERE lobby_id = $1 AND access = true`,
        lobby_id)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby players: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var player LobbyPlayerData
        if err := rows.Scan(&player.LobbyId, &player.PlayerId, &player.IsReady, &player.JoinedAt, &player.Host, &player.Access, &player.InLobby); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan player row: %v", err)
            return sendGenericResponse(response)
        }
        players = append(players, player)
    }

    data := struct {
        LobbyId int64             `json:"lobby_id"`
        Players []LobbyPlayerData `json:"players"`
    }{
        LobbyId: lobby_id,
        Players: players,
    }

    response.Answer = "OK"
    response.Data = data

    log.Printf("Sent lobby players for lobby_id %d: %d players", lobby_id, len(players))
    return sendGenericResponse(response)
}

func handleGetLobbyNames(conn *pgx.Conn, correlation_id string, lobby_id int64) error {
    names := make([]NameData, 0)
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }

    rows, err := conn.Query(context.Background(),
        `SELECT p.id, p.username
         FROM players p
         JOIN lobby_players lp ON p.id = lp.player_id
         WHERE lp.lobby_id = $1 AND lp.access = true`,
        lobby_id)

    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby names: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var username string
         if err := rows.Scan(&id, &username); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan name row: %v", err)
            return sendGenericResponse(response)
        }
        names = append(names, NameData{
            ID:       id,
            IsPlayer: true,
            Name:     username,
        })
    }

    data := struct {
        Names []NameData `json:"names"`
    }{
        Names: names,
    }

    response.Answer = "OK"
    response.Data = data

    log.Printf("Sent lobby names for lobby_id %d: %d players", lobby_id, len(names))
    return sendGenericResponse(response)
}

func handlePrepareToStartLobby(conn *pgx.Conn, correlation_id string, checkData struct {
    LobbyId   int64  `json:"lobby_id"`
    PlayerId  int64  `json:"player_id"`
}) error {
    var lobby LobbyData
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    err = tx.QueryRow(context.Background(),
        `SELECT id, server_id, host_id, is_private, status
         FROM lobbies WHERE id = $1`,
        checkData.LobbyId).Scan(
            &lobby.ID, &lobby.ServerId, &lobby.HostId, &lobby.IsPrivate, &lobby.Status)

    if err != nil {
        tx.Rollback(context.Background())
        if err == pgx.ErrNoRows {
            response.Answer = "Warning"
            response.Error = "DBAnswerLobbyNotFound"
            return sendGenericResponse(response)
        }
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby: %v", err)
        return sendGenericResponse(response)
    }

    if lobby.Status == "STARTED" {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerLobbyCannotStartGameInCurrentStatus"
        return sendGenericResponse(response)
    }

    if lobby.HostId != checkData.PlayerId {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerOnlyHostCanStartGame"
        return sendGenericResponse(response)
    }

    rows, err := tx.Query(context.Background(),
        `SELECT player_id, is_ready FROM lobby_players WHERE lobby_id = $1 AND access = true and in_lobby = true`,
        checkData.LobbyId)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query lobby players: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    players := make([]struct {
        PlayerId int64
        IsReady  bool
    }, 0)

    for rows.Next() {
        var player struct {
            PlayerId int64
            IsReady  bool
        }
        if err := rows.Scan(&player.PlayerId, &player.IsReady); err != nil {
            tx.Rollback(context.Background())
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan player: %v", err)
            return sendGenericResponse(response)
        }
        players = append(players, player)
    }

    if len(players) == 0 {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerNoPlayersInLobby"
        return sendGenericResponse(response)
    }

    for _, player := range players {
        if !player.IsReady {
            tx.Rollback(context.Background())
            response.Answer = "Warning"
            response.Error = "DBAnswerNotAllPlayersAreReady"
            return sendGenericResponse(response)
        }
    }

    _, err = tx.Exec(context.Background(),
        `DELETE FROM lobby_players WHERE lobby_id = $1 AND access = true AND in_lobby = false`,
        checkData.LobbyId)
    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to delete players without in_lobby flag: %v", err)
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        IsReady bool `json:"is_ready"`
    }{
        IsReady: true,
    }


    response.Answer = "OK"
    response.Data = data

    return sendGenericResponse(response)
}

func handleServerInfo(correlation_id string) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "OK",
        Error:         "",
        Data:          struct{}{},
    }

    return sendGenericResponse(response)
}

func handleUnbanPlayer(conn *pgx.Conn, correlation_id string, unbanData struct {
    LobbyId  int64 `json:"lobby_id"`
    HostId    int64  `json:"host_id"`
    PlayerId int64 `json:"player_id"`
}) error {
    var isHost bool
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    tx, err := conn.Begin(context.Background())
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to begin transaction: %v", err)
        return sendGenericResponse(response)
    }

    err = tx.QueryRow(context.Background(),
        `SELECT host FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        unbanData.LobbyId, unbanData.HostId).Scan(&isHost)

    if err != nil {
        if err == pgx.ErrNoRows {
            isHost = false
        } else {
            tx.Rollback(context.Background())
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to query lobby host: %v", err)
            return sendGenericResponse(response)
        }
    }

    if !isHost {
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerOnlyHostCanUnbanPlayer"
        return sendGenericResponse(response)
    }

    result, err := tx.Exec(context.Background(),
        `DELETE FROM lobby_blacklist WHERE lobby_id = $1 AND player_id = $2`,
        unbanData.LobbyId, unbanData.PlayerId)

    if err != nil {
        tx.Rollback(context.Background())
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to remove player from blacklist: %v", err)
        return sendGenericResponse(response)
    }

    if result.RowsAffected() == 0 {
        log.Printf("Player %d not found in blacklist for lobby %d", unbanData.PlayerId, unbanData.LobbyId)
        tx.Rollback(context.Background())
        response.Answer = "Warning"
        response.Error = "DBAnswerPlayerNotBanned"
        return sendGenericResponse(response)
    }

    if err := tx.Commit(context.Background()); err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to commit transaction: %v", err)
        return sendGenericResponse(response)
    }

    data := struct {
        IsBan bool `json:"is_ban"`
    }{
        IsBan: true,
    }

    response.Answer = "OK"
    response.Data = data

    log.Printf("Removed player %d from blacklist for lobby %d", unbanData.PlayerId, unbanData.LobbyId)
    return sendGenericResponse(response)
}

func handleGetBlacklist(conn *pgx.Conn, correlation_id string, inputData struct {
    LobbyId  int64 `json:"lobby_id"`
    HostId    int64  `json:"host_id"`
}) error {
    response := GenericResponse{
        CorrelationId: correlation_id,
        Answer:       "Unknown",
        Error:         "",
        Data:          struct{}{},
    }
    var isHost bool
    err := conn.QueryRow(context.Background(),
        `SELECT host FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        inputData.LobbyId, inputData.HostId).Scan(&isHost)

    if err != nil {
        if err == pgx.ErrNoRows {
            isHost = false
        } else {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to query lobby host: %v", err)
            return sendGenericResponse(response)
        }
    }

    if !isHost {
        response.Answer = "Warning"
        response.Error = "DBAnswerOnlyHostCanUnbanPlayer"
        return sendGenericResponse(response)
    }

    blacklist := make([]BlacklistData, 0)
    bannedPlayerNames := make([]NameData, 0)

    rows, err := conn.Query(context.Background(),
        `SELECT lb.id, lb.lobby_id, lb.player_id, lb.banned_by_id, lb.reason, lb.banned_at,
                p.username
         FROM lobby_blacklist lb
         LEFT JOIN players p ON lb.player_id = p.id
         WHERE lb.lobby_id = $1`,
        inputData.LobbyId)
    if err != nil {
        response.Answer = "Error"
        response.Error = fmt.Sprintf("failed to query blacklist: %v", err)
        return sendGenericResponse(response)
    }
    defer rows.Close()

    for rows.Next() {
        var entry BlacklistData
        var username string
        if err := rows.Scan(&entry.Id, &entry.LobbyId, &entry.PlayerId, &entry.BannedById,
            &entry.Reason, &entry.BannedAt, &username); err != nil {
            response.Answer = "Error"
            response.Error = fmt.Sprintf("failed to scan blacklist entry: %v", err)
            return sendGenericResponse(response)
        }
        blacklist = append(blacklist, entry)

        bannedPlayerNames = append(bannedPlayerNames, NameData{
            ID:       entry.PlayerId,
            IsPlayer: true,
            Name:     username,
        })
    }

    data := struct {
        Blacklist       []BlacklistData `json:"blacklist"`
        BannedPlayers   []NameData     `json:"banned_players"`
    }{
        Blacklist:     blacklist,
        BannedPlayers: bannedPlayerNames,
    }


    response.Answer = "OK"
    response.Data = data
    log.Printf("Sent blacklist for lobby_id %d: %d entries, %d banned players",
        inputData.LobbyId, len(blacklist), len(bannedPlayerNames))
    return sendGenericResponse(response)
}

func sendGenericResponse(response GenericResponse) error {
    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send response: %w", err)
    }

    log.Printf("Sent generic response with correlation_id %s, success: %t, error: %s",
        response.CorrelationId, response.Answer, response.Error)
    return nil
}