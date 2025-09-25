package main

import (
  "context"
  "encoding/json"
  "fmt"
  "log"
  "math/rand"
  "os"
  "time"
  "strings"

  "github.com/confluentinc/confluent-kafka-go/v2/kafka"
  "github.com/jackc/pgx/v5"
)

var computerNames map[string][]string

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

type FeedBackData struct {
    UserName string `json:"username"`
    Message string `json:"message"`
}

type NameData struct {
    ID       int64  `json:"id"`
    IsPlayer bool   `json:"is_player"`
    Name     string `json:"name"`
}

type NamesResponse struct {
    CorrelationId string         `json:"correlation_id"`
    Names         []NameData `json:"names"`
}

type IDResponse struct {
  CorrelationId string `json:"correlation_id"`
  Table string `json:"table"`
  ID    int64 `json:"id"`
}

type AnswerResponse struct {
  CorrelationId string `json:"correlation_id"`
  Success bool `json:"success"`
  ID int64 `json:"id"`
  Error string `json:"error"`
}

type CurrentGameResponse struct {
    CorrelationId string `json:"correlation_id"`
    ID            int64  `json:"id"`
    Finished      bool   `json:"finished"`
}

type RestoreGamesData struct {
    CorrelationId string             `json:"correlation_id"`
    Games         []GameData         `json:"games"`
    Players       []PlayerData       `json:"players"`
    PlayerGames   []PlayerGameData   `json:"player_games"`
    ComputerGames []ComputerGameData `json:"computer_games"`
    History       []GamesHistoryData `json:"history"`
}

type GameReportResponse struct {
    CorrelationId string             `json:"correlation_id"`
    GameId        int64              `json:"game_id"`
    Steps         []GamesHistoryData `json:"steps"`
}

type CurrentPlayersResponse struct {
    CorrelationId string  `json:"correlation_id"`
    GameId        int64   `json:"game_id"`
    PlayerIds     []int64 `json:"player_ids"`
}

type LobbyPlayersResponse struct {
    CorrelationId string            `json:"correlation_id"`
    LobbyId       int64             `json:"lobby_id"`
    Players       []LobbyPlayerData `json:"players"`
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
                err = handleInsertPlayer(conn, player)
                if err != nil {
                    log.Printf("Failed to insert player: %v", err)
                }
            case "change_player":
                var player PlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &player); err != nil {
                    log.Printf("Failed to parse player data: %v", err)
                    continue
                }
                err = handleUpdatePlayer(conn, player)
                if err != nil {
                    log.Printf("Failed to insert player: %v", err)
                }

            case "update_lang_player":
                var player PlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &player); err != nil {
                    log.Printf("Failed to parse player data: %v", err)
                    continue
                }
                err = handleUpdateLangPlayer(conn, player)
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
                err = handleUpdateGame(conn, game)
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
                err = handleAddPlayerGame(conn, playerGame)
                if err != nil {
                    log.Printf("Failed to add player game: %v", err)
                }

            case "feedback":
                var feedback FeedBackData
                if err := json.Unmarshal(kafkaMsg.Data, &feedback); err != nil {
                    log.Printf("Failed to parse feedback data: %v", err)
                    continue
                }
                err = handleFeedback(conn, feedback)
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

            case "set_current_game":
                var playerGame PlayerGameData
                if err := json.Unmarshal(kafkaMsg.Data, &playerGame); err != nil {
                    log.Printf("Failed to parse player game data: %v", err)
                    continue
                }
                err = handleSetCurrentGame(conn, playerGame)
                if err != nil {
                    log.Printf("Failed to set current game: %v", err)
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
                err = handleInsertStep(conn, step)
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
                err = handleLeaveLobby(conn, lobbyPlayerData)

            case "set_player_ready":
                var lobbyPlayerData LobbyPlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &lobbyPlayerData); err != nil {
                    log.Printf("Failed to parse ready data: %v", err)
                    continue
                }
                err = handleSetPlayerReady(conn, lobbyPlayerData)

            case "start_lobby_game":
                var lobbyPlayerData LobbyPlayerData
                if err := json.Unmarshal(kafkaMsg.Data, &lobbyPlayerData); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handleStartLobbyGame(conn, kafkaMsg.CorrelationId, lobbyPlayerData)

            case "get_random_lobby_id":
                var server_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &server_id); err != nil {
                    log.Printf("Failed to parse server id: %v", err)
                    continue
                }
                err = handleGetRandomLobbyId(conn, kafkaMsg.CorrelationId, server_id)

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

            case "check_lobby_ready":
                var lobby_id int64
                if err := json.Unmarshal(kafkaMsg.Data, &lobby_id); err != nil {
                    log.Printf("Failed to parse lobby id: %v", err)
                    continue
                }
                err = handleCheckLobbyReady(conn, kafkaMsg.CorrelationId, lobby_id)

  // another commands

            default:
                log.Printf("Unknown command: %s", kafkaMsg.Command)
        }
    }
}

func handleInsertPlayer(conn *pgx.Conn, player PlayerData) error {
    _, err := conn.Exec(context.Background(),
        `INSERT INTO players(id, firstname, lastname, fullname, username, lang, state)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO NOTHING`,
        player.ID, player.FirstName, player.LastName, player.FullName,
        player.UserName, player.Lang, player.State)

    if err != nil {
        return fmt.Errorf("failed to insert player: %w", err)
    }

    log.Printf("Inserted new player with id %d", player.ID)
    return nil
}

func handleUpdatePlayer(conn *pgx.Conn, player PlayerData) error {
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
        log.Printf("No fields to update for player with id %d", player.ID)
        return nil
    }

    query += fmt.Sprintf(" WHERE id = $%d", argCounter)
    args = append(args, player.ID)

    cmdTag, err := conn.Exec(context.Background(), query, args...)
    if err != nil {
        return fmt.Errorf("failed to update player: %w", err)
    }

    if cmdTag.RowsAffected() == 0 {
        err = handleInsertPlayer(conn, player)
        if err != nil {
            return fmt.Errorf("failed to insert new player: %w", err)
        }
        log.Printf("Created new player with id %d", player.ID)
    } else {
        log.Printf("Updated player with id %d", player.ID)
    }

    return nil
}

func handleUpdateLangPlayer(conn *pgx.Conn, player PlayerData) error {
    _, err := conn.Exec(context.Background(),
        `UPDATE players SET
            lang = $2
        WHERE id = $1`,
        player.ID, player.Lang)

    if err != nil {
        return fmt.Errorf("failed to update lang player: %w", err)
    }

    log.Printf("Updated lang player with id %d", player.ID)

    return nil
}

func handleCreateGame(conn *pgx.Conn, correlation_id string, game GameData) error {
   var newID int64

   err := conn.QueryRow(context.Background(), "SELECT nextval('games_id_seq')").Scan(&newID)
   if err != nil {
    return fmt.Errorf("failed to generate new ID: %w", err)
   }

   _, err = conn.Exec(context.Background(),
    `INSERT INTO games(id, server_id, mode, stage, step, secret_value)
    VALUES($1, $2, $3, $4, $5, $6)`,
    newID, game.ServerId, game.Mode, game.Stage, game.Step, game.SecretValue)

   if err != nil {
    return fmt.Errorf("failed to insert game: %w", err)
   }

   log.Printf("Inserted game with id %d", newID)

   response := IDResponse{
    CorrelationId: correlation_id,
    Table:    "games",
    ID:    newID,
   }

   responseBytes, err := json.Marshal(response)
   if err != nil {
    return fmt.Errorf("failed to marshal ID response: %w", err)
   }

   producer_topic := "db_bot"
   err = producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
    Value:          responseBytes,
   }, nil)

   if err != nil {
    return fmt.Errorf("failed to send ID response: %w", err)
   }

   return nil
}

func handleUpdateGame(conn *pgx.Conn, game GameData) error {
  var err error
  if game.SecretValue == 0 {
  _, err = conn.Exec(context.Background(),
    `UPDATE games SET
    server_id = $2,
    stage = $3,
    step = $4
    WHERE id = $1`,
    game.ID, game.ServerId, game.Stage, game.Step)
  } else {
  _, err = conn.Exec(context.Background(),
    `UPDATE games SET
    server_id = $2,
    stage = $3,
    step = $4,
    secret_value = $5
    WHERE id = $1`,
    game.ID, game.ServerId, game.Stage, game.Step, game.SecretValue)
  }
  if err != nil {
  return fmt.Errorf("failed to update game: %w", err)
  }

  log.Printf("Updated game with id %d", game.ID)
  return nil
}

func handleCreateComputer(conn *pgx.Conn, correlation_id string, computer ComputerGameData) error {
    var newID int64

    err := conn.QueryRow(context.Background(), "SELECT nextval('computers_id_seq')").Scan(&newID)
    if err != nil {
        return fmt.Errorf("failed to generate new ID: %w", err)
    }

    names, ok := computerNames[computer.GameBrain]
    if !ok {
        names = []string{"Unknown"}
    }
    rand.Seed(time.Now().UnixNano())

    rand_i := rand.Intn(len(names))
    computer.Name = names[rand_i]

    _, err = conn.Exec(context.Background(),
        `INSERT INTO computers(computer_id, player_id, server_id, game_id, game_brain, name)
        VALUES($1, $2, $3, $4, $5, $6)`,
        newID, computer.PlayerId, computer.ServerId, computer.GameId, computer.GameBrain, computer.Name)

    if err != nil {
        return fmt.Errorf("failed to insert computer: %w", err)
    }

    log.Printf("Inserted computer with id %d and name %s", newID, computer.Name)

    response := IDResponse{
        CorrelationId: correlation_id,
        Table:        "computers",
        ID:           newID,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal ID response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send ID response: %w", err)
    }

    return nil
}

func handleAddPlayerGame(conn *pgx.Conn, playerGame PlayerGameData) error {
    _, err := conn.Exec(context.Background(),
    `INSERT INTO player_games(player_id, server_id, game_id, is_current_game, is_host)
    VALUES($1, $2, $3, $4, $5)`,
    playerGame.PlayerId, playerGame.ServerId, playerGame.GameId, playerGame.IsCurrentGame, playerGame.IsHost)

    if err != nil {
    return fmt.Errorf("failed to insert player game: %w", err)
    }

    log.Printf("Inserted player game")
    return nil
}

func handleFeedback(conn *pgx.Conn, feedback FeedBackData) error {
    _, err := conn.Exec(context.Background(),
    `INSERT INTO feedback(username, message)
    VALUES($1, $2)`,
    feedback.UserName, feedback.Message)

    if err != nil {
    return fmt.Errorf("failed to add feedback: %w", err)
    }

    log.Printf("Added feedback")
    return nil
}

func handleGetCurrentGame(conn *pgx.Conn, correlation_id string, player_id int64) error {
    var gameID int64
    var stage string
    var finished bool

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
            return fmt.Errorf("failed to query current game: %w", err)
        }
    } else {
        finished = (stage == "FINISHED")
    }

    response := CurrentGameResponse{
        CorrelationId: correlation_id,
        ID:            gameID,
        Finished:      finished,
    }

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

    log.Printf("Sent current game ID %d for player %d (finished: %v)", gameID, player_id, finished)
    return nil
}

func handleSetCurrentGame(conn *pgx.Conn, playerGame PlayerGameData) error {
    tx, err := conn.Begin(context.Background())
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback(context.Background())

    _, err = tx.Exec(context.Background(),
        `UPDATE player_games
            SET is_current_game = false
            WHERE player_id = $1`,
        playerGame.PlayerId)
    if err != nil {
        return fmt.Errorf("failed to reset current games: %w", err)
    }

    _, err = tx.Exec(context.Background(),
        `UPDATE player_games
            SET is_current_game = $1
            WHERE player_id = $2 AND game_id = $3`,
        playerGame.IsCurrentGame, playerGame.PlayerId, playerGame.GameId)
    if err != nil {
        return fmt.Errorf("failed to set current game: %w", err)
    }

    if err := tx.Commit(context.Background()); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    log.Printf("Set current game for player %d to game %d (is_current=%v)",
        playerGame.PlayerId, playerGame.GameId, playerGame.IsCurrentGame)
    return nil
}

func handleGetServerGames(conn *pgx.Conn, correlation_id string, server_id int64) error {
    games := make([]GameData, 0)
    rows, err := conn.Query(context.Background(),
        `SELECT id, server_id, mode, stage, step, secret_value
         FROM games
         WHERE server_id = $1 AND stage != 'FINISHED'`,
        server_id)
    if err != nil {
        return fmt.Errorf("failed to query games: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var game GameData
        if err := rows.Scan(&game.ID, &game.ServerId, &game.Mode, &game.Stage, &game.Step, &game.SecretValue); err != nil {
            return fmt.Errorf("failed to scan game row: %w", err)
        }
        games = append(games, game)
    }

    players := make([]PlayerData, 0)
    rows, err = conn.Query(context.Background(),
        `SELECT id, firstname, lastname, fullname, username, lang, state
         FROM players`)
    if err != nil {
        return fmt.Errorf("failed to query players: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var pg PlayerData
        if err := rows.Scan(&pg.ID, &pg.FirstName, &pg.LastName, &pg.FullName, &pg.UserName, &pg.Lang, &pg.State); err != nil {
            return fmt.Errorf("failed to scan players row: %w", err)
        }
        players = append(players, pg)
    }


    playerGames := make([]PlayerGameData, 0)
    rows, err = conn.Query(context.Background(),
        `SELECT player_id, server_id, game_id, is_current_game, is_host
         FROM player_games
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )`, server_id)
    if err != nil {
        return fmt.Errorf("failed to query player_games: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var pg PlayerGameData
        if err := rows.Scan(&pg.PlayerId, &pg.ServerId, &pg.GameId, &pg.IsCurrentGame, &pg.IsHost); err != nil {
            return fmt.Errorf("failed to scan player_games row: %w", err)
        }
        playerGames = append(playerGames, pg)
    }

    computerGames := make([]ComputerGameData, 0)
    rows, err = conn.Query(context.Background(),
        `SELECT computer_id, player_id, server_id, game_id, game_brain, name
         FROM computers
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )`, server_id)
    if err != nil {
        return fmt.Errorf("failed to query computers: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var cg ComputerGameData
        if err := rows.Scan(&cg.ComputerId, &cg.PlayerId, &cg.ServerId, &cg.GameId, &cg.GameBrain, &cg.Name); err != nil {
            return fmt.Errorf("failed to scan computer row: %w", err)
        }
        computerGames = append(computerGames, cg)
    }

    history := make([]GamesHistoryData, 0)
    rows, err = conn.Query(context.Background(),
        `SELECT game_id, player_id, server_id, step, game_value, bulls, cows, is_computer, is_give_up, timestamp
         FROM games_history
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )
         ORDER BY timestamp ASC`, server_id)
    if err != nil {
        return fmt.Errorf("failed to query games history: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var h GamesHistoryData
        if err := rows.Scan(&h.GameId, &h.PlayerId, &h.ServerId, &h.Step, &h.GameValue, &h.Bulls, &h.Cows, &h.IsComputer, &h.IsGiveUp, &h.Timestamp); err != nil {
            return fmt.Errorf("failed to scan history row: %w", err)
        }
        history = append(history, h)
    }

    response := RestoreGamesData{
        CorrelationId: correlation_id,
        Games:         games,
        Players:       players,
        PlayerGames:   playerGames,
        ComputerGames: computerGames,
        History:       history,
    }

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

    log.Printf("Sent server games data for server_id %d (with computers and history)", server_id)
    return nil
}

func handleGetGameNames(conn *pgx.Conn, correlationId string, gameId int64) error {
    var names []NameData

    rows, err := conn.Query(context.Background(),
        `SELECT p.id, p.username
         FROM players p
         JOIN player_games pg ON p.id = pg.player_id
         WHERE pg.game_id = $1`, gameId)
    if err != nil {
        return fmt.Errorf("failed to query players: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var username string
        if err := rows.Scan(&id, &username); err != nil {
            return fmt.Errorf("failed to scan player row: %w", err)
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
        return fmt.Errorf("failed to query computers: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            return fmt.Errorf("failed to scan computer row: %w", err)
        }
        names = append(names, NameData{
            ID:       id,
            IsPlayer: false,
            Name:     name,
        })
    }

    response := NamesResponse{
        CorrelationId: correlationId,
        Names:         names,
    }

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

    log.Printf("Sent game names for game_id %d", gameId)
    return nil
}

func handleGetGameReport(conn *pgx.Conn, correlationId string, gameId int64) error {
    var steps []GamesHistoryData

    rows, err := conn.Query(context.Background(),
        `SELECT game_id, player_id, server_id, step, game_value, bulls, cows, is_computer, is_give_up, timestamp
         FROM games_history
         WHERE game_id = $1
         ORDER BY timestamp ASC`, gameId)
    if err != nil {
        return fmt.Errorf("failed to query game steps: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var step GamesHistoryData
        if err := rows.Scan(&step.GameId, &step.PlayerId, &step.ServerId, &step.Step, &step.GameValue, &step.Bulls, &step.Cows, &step.IsComputer, &step.IsGiveUp, &step.Timestamp); err != nil {
            return fmt.Errorf("failed to scan step row: %w", err)
        }
        steps = append(steps, step)
    }

    response := GameReportResponse{
        CorrelationId: correlationId,
        GameId:        gameId,
        Steps:         steps,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal game report response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send game report response: %w", err)
    }

    log.Printf("Sent game report for game_id %d with %d steps", gameId, len(steps))
    return nil
}

func handleCreateLobby(conn *pgx.Conn, correlation_id string, lobby LobbyData) error {
    var newID int64
    err := conn.QueryRow(context.Background(), "SELECT nextval('lobbies_id_seq')").Scan(&newID)
    if err != nil {
        return fmt.Errorf("failed to generate new ID: %w", err)
    }

    _, err = conn.Exec(context.Background(),
        `INSERT INTO lobbies(id, server_id, host_id, game_id, is_private, password, status)
        VALUES($1, $2, $3, $4, $5, $6, $7)`,
        newID, lobby.ServerId, lobby.HostId, lobby.GameId, lobby.IsPrivate, lobby.Password, "WAITING")

    if err != nil {
        return fmt.Errorf("failed to insert lobby: %w", err)
    }

    _, err = conn.Exec(context.Background(),
        `INSERT INTO lobby_players(lobby_id, player_id, is_ready, joined_at, host, access)
        VALUES($1, $2, $3, $4, $5, $6)`,
        newID, lobby.HostId, false, time.Now(), true, true)

    if err != nil {
        return fmt.Errorf("failed to add creator to lobby: %w", err)
    }

    response := IDResponse{
        CorrelationId: correlation_id,
        Table:        "lobbies",
        ID:           newID,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal ID response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    return err
}

func handleJoinLobby(conn *pgx.Conn, correlation_id string, joinData struct {
    LobbyId   int64  `json:"lobby_id"`
    PlayerId  int64  `json:"player_id"`
    Password  string `json:"password,omitempty"`
}) error {
    tx, err := conn.Begin(context.Background())
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback(context.Background())

    var lobby LobbyData
    err = tx.QueryRow(context.Background(),
        `SELECT id, server_id, host_id, game_id, is_private, password, status
         FROM lobbies WHERE id = $1`,
        joinData.LobbyId).Scan(
            &lobby.ID, &lobby.ServerId, &lobby.HostId, &lobby.GameId, &lobby.IsPrivate,
            &lobby.Password, &lobby.Status)

    if err != nil {
        if err == pgx.ErrNoRows {
            return sendAnswerResponse(correlation_id, false, "DBAnswerLobbyNotFound")
        }
        return fmt.Errorf("failed to query lobby: %w", err)
    }

    hasAccess := true
    if lobby.IsPrivate {
        if lobby.Password != joinData.Password {
            if joinData.Password == "" {
                hasAccess = false
            } else {
                return sendAnswerResponse(correlation_id, false, "DBAnswerInvalidPassword")
            }
        }
    }

    if lobby.Status != "WAITING" {
        return sendAnswerResponse(correlation_id, false, "DBAnswerLobbyIsntAcceptingPlayers")
    }

    var existingPlayer int64
    var existingAccess bool
    err = tx.QueryRow(context.Background(),
        `SELECT player_id, access FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        joinData.LobbyId, joinData.PlayerId).Scan(&existingPlayer, &existingAccess)

    if err == nil {
        if hasAccess && !existingAccess {
            _, err = tx.Exec(context.Background(),
                `UPDATE lobby_players SET access = true WHERE lobby_id = $1 AND player_id = $2`,
                joinData.LobbyId, joinData.PlayerId)
            if err != nil {
                return fmt.Errorf("failed to update player access: %w", err)
            }
            log.Printf("Updated player %d access to true in lobby %d", joinData.PlayerId, joinData.LobbyId)
        }
    } else {
        if err == pgx.ErrNoRows {
            _, err = tx.Exec(context.Background(),
                `INSERT INTO lobby_players(lobby_id, player_id, is_ready, joined_at, host, access)
                VALUES($1, $2, $3, $4, $5, $6)`,
                joinData.LobbyId, joinData.PlayerId, false, time.Now(), false, hasAccess)

            if err != nil {
                return fmt.Errorf("failed to add player to lobby: %w", err)
            }
            if !hasAccess {
                if err := tx.Commit(context.Background()); err != nil {
                    return fmt.Errorf("failed to commit transaction: %w", err)
                }
                return sendAnswerResponse(correlation_id, false, "DBAnswerPasswordNeeded")
            }

        } else {
            return fmt.Errorf("failed to check player existence: %w", err)
        }
    }

    if err := tx.Commit(context.Background()); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    response := AnswerResponse{
        CorrelationId: correlation_id,
        Success:       true,
        ID:            lobby.GameId,
        Error:         "",
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal join lobby response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send join lobby response: %w", err)
    }

    log.Printf("Player %d joined lobby %d with game_id %d", joinData.PlayerId, joinData.LobbyId, lobby.GameId)
    return nil
}

func handleLeaveLobby(conn *pgx.Conn, lobbyPlayerData LobbyPlayerData) error {
    tx, err := conn.Begin(context.Background())
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback(context.Background())

    var hostId int64
    err = tx.QueryRow(context.Background(),
        `SELECT host_id FROM lobbies WHERE id = $1`,
        lobbyPlayerData.LobbyId).Scan(&hostId)

    if err != nil {
        return fmt.Errorf("failed to get lobby info: %w", err)
    }

    result, err := tx.Exec(context.Background(),
        `DELETE FROM lobby_players WHERE lobby_id = $1 AND player_id = $2`,
        lobbyPlayerData.LobbyId, lobbyPlayerData.PlayerId)

    if err != nil {
        return fmt.Errorf("failed to remove player from lobby: %w", err)
    }

    if result.RowsAffected() == 0 {
        log.Printf("Player %d not found in lobby %d", lobbyPlayerData.PlayerId, lobbyPlayerData.LobbyId)
        return nil
    }

    var playerCount int
    err = tx.QueryRow(context.Background(),
        `SELECT COUNT(*) FROM lobby_players WHERE lobby_id = $1`,
        lobbyPlayerData.LobbyId).Scan(&playerCount)

    if err != nil {
        return fmt.Errorf("failed to check player count: %w", err)
    }

    if playerCount == 0 {
        _, err = tx.Exec(context.Background(),
            `DELETE FROM lobbies WHERE id = $1`,
            lobbyPlayerData.LobbyId)
        if err != nil {
            return fmt.Errorf("failed to delete empty lobby: %w", err)
        }
        log.Printf("Deleted empty lobby %d", lobbyPlayerData.LobbyId)
    } else {
        if hostId == lobbyPlayerData.PlayerId {
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
                    return fmt.Errorf("failed to find new host: %w", err)
                }
            } else {
                _, err = tx.Exec(context.Background(),
                    `UPDATE lobbies SET host_id = $1 WHERE id = $2`,
                    newHostId, lobbyPlayerData.LobbyId)

                if err != nil {
                    return fmt.Errorf("failed to update lobby host: %w", err)
                }
                log.Printf("Transferred host from player %d to player %d in lobby %d",
                    lobbyPlayerData.PlayerId, newHostId, lobbyPlayerData.LobbyId)
            }
        }
    }

    if err := tx.Commit(context.Background()); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    log.Printf("Player %d left lobby %d", lobbyPlayerData.PlayerId, lobbyPlayerData.LobbyId)
    return nil
}

func handleSetPlayerReady(conn *pgx.Conn, lobbyPlayerData LobbyPlayerData) error {
    result, err := conn.Exec(context.Background(),
        `UPDATE lobby_players SET is_ready = $1
         WHERE lobby_id = $2 AND player_id = $3`,
        lobbyPlayerData.IsReady, lobbyPlayerData.LobbyId, lobbyPlayerData.PlayerId)

    if err != nil {
        return fmt.Errorf("failed to update player readiness: %w", err)
    }

    if result.RowsAffected() == 0 {
        return fmt.Errorf("player %d not found in lobby %d", lobbyPlayerData.PlayerId, lobbyPlayerData.LobbyId)
    }

    log.Printf("Player %d readiness set to %t in lobby %d",
        lobbyPlayerData.PlayerId, lobbyPlayerData.IsReady, lobbyPlayerData.LobbyId)
    return nil
}

func handleStartLobbyGame(conn *pgx.Conn, correlation_id string, lobbyPlayerData LobbyPlayerData) error {
    tx, err := conn.Begin(context.Background())
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback(context.Background())

    var lobby LobbyData
    err = tx.QueryRow(context.Background(),
        `SELECT id, server_id, host_id, game_id, is_private, status
         FROM lobbies WHERE id = $1`,
        lobbyPlayerData.LobbyId).Scan(
            &lobby.ID, &lobby.ServerId, &lobby.HostId, &lobby.GameId, &lobby.IsPrivate, &lobby.Status)

    if err != nil {
        return fmt.Errorf("failed to query lobby: %w", err)
    }

    if lobby.HostId != lobbyPlayerData.PlayerId {
        return sendAnswerResponse(correlation_id, false, "DBAnswerOnlyHostCanStartGame")
    }

    if lobby.Status != "WAITING" {
        return sendAnswerResponse(correlation_id, false, "DBAnswerLobbyCannotStartGameInCurrentStatus")
    }

    rows, err := tx.Query(context.Background(),
        `SELECT player_id, is_ready FROM lobby_players WHERE lobby_id = $1`,
        lobbyPlayerData.LobbyId)

    if err != nil {
        return fmt.Errorf("failed to query lobby players: %w", err)
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
            return fmt.Errorf("failed to scan player: %w", err)
        }
        players = append(players, player)
    }

    for _, player := range players {
        if !player.IsReady {
            return sendAnswerResponse(correlation_id, false, "DBAnswerNotAllPlayersAreReady")
        }
    }

    _, err = tx.Exec(context.Background(),
        `UPDATE lobbies SET status = 'STARTED' WHERE id = $1`,
        lobbyPlayerData.LobbyId)

    if err != nil {
        return fmt.Errorf("failed to update lobby status: %w", err)
    }

    if err := tx.Commit(context.Background()); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    response := AnswerResponse{
        CorrelationId: correlation_id,
        Success:       true,
        ID:            lobby.GameId,
        Error:         "",
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    log.Printf("Host player %d started game %d from lobby %d with %d players",
        lobbyPlayerData.PlayerId, lobby.GameId, lobbyPlayerData.LobbyId, len(players))
    return err
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
                access BOOLEAN DEFAULT false
            )`)
        if err != nil {
            return err
        }
        log.Println("Table 'lobby_players' created successfully")
    }

    return nil
}

func handleGetCurrentPlayers(conn *pgx.Conn, correlationId string, gameId int64) error {
    var playerIds []int64

    rows, err := conn.Query(context.Background(),
        `SELECT player_id
         FROM player_games
         WHERE game_id = $1 AND is_current_game = true`,
        gameId)
    if err != nil {
        return fmt.Errorf("failed to query current players: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var playerId int64
        if err := rows.Scan(&playerId); err != nil {
            return fmt.Errorf("failed to scan player id: %w", err)
        }
        playerIds = append(playerIds, playerId)
    }

    response := CurrentPlayersResponse{
        CorrelationId: correlationId,
        GameId:        gameId,
        PlayerIds:     playerIds,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal current players response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send current players response: %w", err)
    }

    log.Printf("Sent current players for game_id %d: %v", gameId, playerIds)
    return nil
}

func handleInsertStep(conn *pgx.Conn, step GamesHistoryData) error {
  _, err := conn.Exec(context.Background(),
   `INSERT INTO games_history(game_id, player_id, server_id, step, game_value, bulls, cows, is_computer, is_give_up, timestamp)
   VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
   step.GameId, step.PlayerId, step.ServerId, step.Step, step.GameValue, step.Bulls, step.Cows, step.IsComputer, step.IsGiveUp, step.Timestamp)

   if err != nil {
     return fmt.Errorf("failed to insert step: %w", err)
   }

    log.Printf("Inserted step with game id %d at %v", step.GameId, step.Timestamp)
    return nil
}

func sendAnswerResponse(correlation_id string, success bool, errorMessage string) error {
    response := AnswerResponse{
        CorrelationId: correlation_id,
        Success:        success,
        ID:             0,
        Error:          errorMessage,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal answer response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    log.Printf("Answer response sent: %s", errorMessage)
    return err
}

func handleGetRandomLobbyId(conn *pgx.Conn, correlationId string, serverId int64) error {
    var lobbyId int64
    err := conn.QueryRow(context.Background(),
        `SELECT id FROM lobbies
         WHERE server_id = $1 AND is_private = false AND status = 'WAITING'
         ORDER BY RANDOM() LIMIT 1`,
        serverId).Scan(&lobbyId)

    if err != nil {
        if err == pgx.ErrNoRows {
            lobbyId = 0
        } else {
            return fmt.Errorf("failed to query random lobby: %w", err)
        }
    }

    response := IDResponse{
        CorrelationId: correlationId,
        Table:        "lobbies",
        ID:           lobbyId,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal random lobby response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send random lobby response: %w", err)
    }

    log.Printf("Sent random lobby ID %d for server_id %d", lobbyId, serverId)
    return nil
}

func handleGetLobbyId(conn *pgx.Conn, correlationId string, player_id int64) error {
    var lobbyId int64
    err := conn.QueryRow(context.Background(),
        `SELECT lobby_id FROM lobby_players WHERE player_id = $1 LIMIT 1`,
        player_id).Scan(&lobbyId)

    if err != nil {
        if err == pgx.ErrNoRows {
            lobbyId = 0
        } else {
            return fmt.Errorf("failed to query player lobby: %w", err)
        }
    }

    response := IDResponse{
        CorrelationId: correlationId,
        Table:        "lobby_players",
        ID:           lobbyId,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal lobby response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send lobby response: %w", err)
    }

    log.Printf("Sent lobby ID %d for player_id %d", lobbyId, player_id)
    return nil
}

func handleGetLobbyPlayers(conn *pgx.Conn, correlationId string, lobby_id int64) error {
    var players []LobbyPlayerData

    rows, err := conn.Query(context.Background(),
        `SELECT lobby_id, player_id, is_ready, joined_at, host, access
         FROM lobby_players
         WHERE lobby_id = $1 AND access = true`,
        lobby_id)
    if err != nil {
        return fmt.Errorf("failed to query lobby players: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var player LobbyPlayerData
        if err := rows.Scan(&player.LobbyId, &player.PlayerId, &player.IsReady, &player.JoinedAt, &player.Host, &player.Access); err != nil {
            return fmt.Errorf("failed to scan lobby player: %w", err)
        }
        players = append(players, player)
    }

    response := LobbyPlayersResponse{
        CorrelationId: correlationId,
        LobbyId:       lobby_id,
        Players:       players,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal lobby players response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send lobby players response: %w", err)
    }

    log.Printf("Sent lobby players for lobby_id %d: %d players", lobby_id, len(players))
    return nil
}

func handleGetLobbyNames(conn *pgx.Conn, correlationId string, lobby_id int64) error {
    var names []NameData

    rows, err := conn.Query(context.Background(),
        `SELECT p.id, p.username
         FROM players p
         JOIN lobby_players lp ON p.id = lp.player_id
         WHERE lp.lobby_id = $1 AND lp.access = true`,
        lobby_id)
    if err != nil {
        return fmt.Errorf("failed to query lobby players: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var username string
        if err := rows.Scan(&id, &username); err != nil {
            return fmt.Errorf("failed to scan player row: %w", err)
        }
        names = append(names, NameData{
            ID:       id,
            IsPlayer: true,
            Name:     username,
        })
    }

    response := NamesResponse{
        CorrelationId: correlationId,
        Names:         names,
    }

    responseBytes, err := json.Marshal(response)
    if err != nil {
        return fmt.Errorf("failed to marshal lobby names response: %w", err)
    }

    producer_topic := "db_bot"
    err = producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &producer_topic, Partition: kafka.PartitionAny},
        Value:          responseBytes,
    }, nil)

    if err != nil {
        return fmt.Errorf("failed to send lobby names response: %w", err)
    }

    log.Printf("Sent lobby names for lobby_id %d: %d players", lobby_id, len(names))
    return nil
}

func handleCheckLobbyReady(conn *pgx.Conn, correlationId string, lobby_id int64) error {
    var lobby LobbyData
    err := conn.QueryRow(context.Background(),
        `SELECT id, server_id, host_id, is_private, status
         FROM lobbies WHERE id = $1`,
        lobby_id).Scan(
            &lobby.ID, &lobby.ServerId, &lobby.HostId, &lobby.IsPrivate, &lobby.Status)

    if err != nil {
        if err == pgx.ErrNoRows {
            return sendAnswerResponse(correlationId, false, "DBAnswerLobbyNotFound")
        }
        return fmt.Errorf("failed to query lobby: %w", err)
    }

    if lobby.Status != "WAITING" {
        return sendAnswerResponse(correlationId, false, "DBAnswerLobbyCannotStartGameInCurrentStatus")
    }

    rows, err := conn.Query(context.Background(),
        `SELECT player_id, is_ready FROM lobby_players WHERE lobby_id = $1 AND access = true`,
        lobby_id)

    if err != nil {
        return fmt.Errorf("failed to query lobby players: %w", err)
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
            return fmt.Errorf("failed to scan player: %w", err)
        }
        players = append(players, player)
    }

    if len(players) == 0 {
        return sendAnswerResponse(correlationId, false, "DBAnswerNoPlayersInLobby")
    }

    for _, player := range players {
        if !player.IsReady {
            return sendAnswerResponse(correlationId, false, "DBAnswerNotAllPlayersAreReady")
        }
    }

    return sendAnswerResponse(correlationId, true, "")
}