package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

type TaskType int
type TaskStatus int
type ScanDirection int

const (
	TaskMatchHistoryScan TaskType = iota
	TaskPlayerStatsUpdate
	TaskMatchResultInsert
	TaskMatchEventsInsert
	TaskTeamEncounterInsert
)

const (
	TaskQueued TaskStatus = iota
	TaskDeferred
	TaskActive
	TaskBlocked
)

const (
	ScanFromLowerBound ScanDirection = iota
	ScanFromUpperBound
)

type Task struct {
	ID       int
	Status   TaskStatus
	Priority int
	Updated  time.Time
	Type     TaskType
	Data     TaskData
}

type TaskData struct {
	MatchID       int           `json:"MatchID,omitempty"`
	MatchUUID     string        `json:"MatchUUID,omitempty"`
	PlayerID      int           `json:"PlayerID,omitempty"`
	Gamertag      string        `json:"Gamertag,omitempty"`
	MatchType     string        `json:"MatchType,omitempty"`
	LowerBound    int           `json:"LowerBound,omitempty"`
	UpperBound    int           `json:"UpperBound,omitempty"`
	ScanDirection ScanDirection `json:"ScanDirection,omitempty"`
	FullScan      bool          `json:"FullScan,omitempty"`
}

func (ds *DataStore) postTask(task *Task) (err error) {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Marshal the task data
	data, err := json.Marshal(task.Data)
	if err != nil {
		return err
	}

	// Insert the task
	_, err = tx.Exec(`
		INSERT INTO task (
			t_status,
			t_priority,
			t_updated,
			t_type,
			t_data
		) VALUES ($1, $2, now(), $3, $4)
	`, TaskQueued, task.Priority, task.Type, data)
	return err
}

func (ds *DataStore) startTask() (*Task, error) {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Get a task
	// https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/
	result := tx.QueryRow(`
		UPDATE task
		SET t_status = $1
		WHERE t_id = (
			SELECT t_id
			FROM task
			WHERE t_status != $1
			AND t_status != $2
			ORDER BY t_status DESC, t_priority DESC, t_updated ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING t_id, t_status, t_priority, t_updated, t_type, t_data
	`, TaskActive, TaskBlocked)

	// Scan the result
	task := new(Task)
	var data []byte
	err = result.Scan(&task.ID, &task.Status, &task.Priority, &task.Updated, &task.Type, &data)
	if err != nil {
		return nil, err
	}

	// Decode the data
	err = json.Unmarshal(data, &task.Data)
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (ds *DataStore) blockTask(task *Task) error {
	task.Status = TaskBlocked
	return ds.updateTask(task)
}

func (ds *DataStore) deferTask(task *Task) error {
	task.Status = TaskDeferred
	return ds.updateTask(task)
}

func (ds *DataStore) updateTask(task *Task) (err error) {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Marshal the task data
	data, err := json.Marshal(&task.Data)
	if err != nil {
		return err
	}

	// Update the task
	_, err = tx.Exec(`
		UPDATE task
		SET
			t_status = $2,
			t_priority = $3,
			t_updated = now(),
			t_type = $4,
			t_data = $5
		WHERE t_id = $1
		RETURNING t_id
	`, task.ID, task.Status, task.Priority, task.Type, data)
	return err
}

func (ds *DataStore) finishTask(task *Task) (err error) {
	// Create transaction
	ctx := context.Background()
	txOpts := new(sql.TxOptions)
	tx, err := ds.db.BeginTx(ctx, txOpts)
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			tx.Commit()
			return
		}
		tx.Rollback()
	}()

	// Delete the task
	_, err = tx.Exec("DELETE FROM task WHERE t_id = $1", task.ID)
	return err
}
