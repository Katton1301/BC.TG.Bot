package main

import (
  "context"
  "encoding/json"
  "fmt"
  "log"
  "math/rand"
  "os"
  "time"

  "github.com/confluentinc/confluent-kafka-go/v2/kafka"
  "github.com/jackc/pgx/v5"
)

var computerNames map[string][]string

type PlayerData struct {
  Id   int64  `json:"player_id"`
  FirstName string `json:"firstname"`
  LastName string `json:"lastname"`
  FullName string `json:"fullname"`
  UserName string `json:"username"`
  Lang string `json:"lang"`
  State string `json:"state"`
}

type GameData struct {
  Id          int64  `json:"id"`
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
}

type GameNameData struct {
    Id       int64  `json:"id"`
    IsPlayer bool   `json:"is_player"`
    Name     string `json:"name"`
}

type GameNamesResponse struct {
    CorrelationId string         `json:"correlation_id"`
    Names         []GameNameData `json:"names"`
}

type IDResponse struct {
  CorrelationId string `json:"correlation_id"`
  Table string `json:"table"`
  ID    int64 `json:"id"`
}

type RestoreGamesData struct {
    CorrelationId string             `json:"correlation_id"`
    Games         []GameData         `json:"games"`
    Players       []PlayerData       `json:"players"`
    PlayerGames   []PlayerGameData   `json:"player_games"`
    ComputerGames []ComputerGameData `json:"computer_games"`
    History       []GamesHistoryData `json:"history"`
}

type KafkaMessage struct {
  Command string          `json:"command"`
  CorrelationId string      `json:"correlation_id"`
  Data    json.RawMessage `json:"data"`
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

var producer *kafka.Producer

func main() {
 conn, err := pgx.Connect(context.Background(),
  "postgres://"+os.Getenv("POSTGRES_USER")+":"+os.Getenv("POSTGRES_PASSWORD")+
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
  case "change_player":
   var player PlayerData
   if err := json.Unmarshal(kafkaMsg.Data, &player); err != nil {
    log.Printf("Failed to parse player data: %v", err)
    continue
   }
   err = handleInsertPlayer(conn, player)
   if err != nil {
    log.Printf("Failed to insert player: %v", err)
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
  ON CONFLICT (id) DO UPDATE SET
   firstname = EXCLUDED.firstname,
   lastname = EXCLUDED.lastname,
   fullname = EXCLUDED.fullname,
   username = EXCLUDED.username,
   lang = EXCLUDED.lang,
   state = EXCLUDED.state`,
  player.Id, player.FirstName, player.LastName, player.FullName, player.UserName, player.Lang, player.State)

  if err != nil {
    return fmt.Errorf("failed to upsert player: %w", err)
  }

   log.Printf("Upserted player with id %d", player.Id)
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
    game.Id, game.ServerId, game.Stage, game.Step)
  } else {
  _, err = conn.Exec(context.Background(),
    `UPDATE games SET
    server_id = $2,
    stage = $3,
    step = $4,
    secret_value = $5
    WHERE id = $1`,
    game.Id, game.ServerId, game.Stage, game.Step, game.SecretValue)
  }
  if err != nil {
  return fmt.Errorf("failed to update game: %w", err)
  }

  log.Printf("Updated game with id %d", game.Id)
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

   func handleGetCurrentGame(conn *pgx.Conn, correlation_id string, player_id int64) error {
    var gameID int64

    err := conn.QueryRow(context.Background(),
        `SELECT game_id FROM player_games
         WHERE player_id = $1 AND is_current_game = true`,
        player_id).Scan(&gameID)

    if err != nil {
        if err == pgx.ErrNoRows {
            gameID = 0
        } else {
            return fmt.Errorf("failed to query current game: %w", err)
        }
    }

    response := IDResponse{
        CorrelationId: correlation_id,
        Table:         "games",
        ID:            gameID,
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

    log.Printf("Sent current game ID %d for player %d", gameID, player_id)
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
        if err := rows.Scan(&game.Id, &game.ServerId, &game.Mode, &game.Stage, &game.Step, &game.SecretValue); err != nil {
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
        if err := rows.Scan(&pg.Id, &pg.FirstName, &pg.LastName, &pg.FullName, &pg.UserName, &pg.Lang, &pg.State); err != nil {
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
        `SELECT game_id, player_id, server_id, step, game_value, bulls, cows, is_computer
         FROM games_history
         WHERE server_id = $1 AND game_id IN (
             SELECT id FROM games WHERE server_id = $1 AND stage != 'FINISHED'
         )`, server_id)
    if err != nil {
        return fmt.Errorf("failed to query games history: %w", err)
    }
    defer rows.Close()

    for rows.Next() {
        var h GamesHistoryData
        if err := rows.Scan(&h.GameId, &h.PlayerId, &h.ServerId, &h.Step, &h.GameValue, &h.Bulls, &h.Cows, &h.IsComputer); err != nil {
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
    var names []GameNameData

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
        names = append(names, GameNameData{
            Id:       id,
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
        names = append(names, GameNameData{
            Id:       id,
            IsPlayer: false,
            Name:     name,
        })
    }

    response := GameNamesResponse{
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
      is_computer BOOLEAN
     )`)
    if err != nil {
     return err
    }
    log.Println("Table 'games_history' created successfully")
  }

    return nil
}

func handleInsertStep(conn *pgx.Conn, step GamesHistoryData) error {
  _, err := conn.Exec(context.Background(),
   `INSERT INTO games_history(game_id, player_id, server_id, step, game_value, bulls, cows, is_computer)
   VALUES($1, $2, $3, $4, $5, $6, $7, $8)`,
   step.GameId, step.PlayerId, step.ServerId, step.Step, step.GameValue, step.Bulls, step.Cows, step.IsComputer)

   if err != nil {
     return fmt.Errorf("failed to insert step: %w", err)
   }

    log.Printf("Inserted step with game id %d", step.GameId)
    return nil
}