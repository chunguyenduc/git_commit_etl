package github

import "time"

type CommitResponse struct {
	Sha    string  `json:"sha"`
	Commit *Commit `json:"commit"`
	Author *Author `json:"author"`
}

type Author struct {
	Login string `json:"login"`
	ID    int    `json:"id"`
}

type Commit struct {
	Author *CommitAuthor `json:"author"`
}

type CommitAuthor struct {
	Name  string    `json:"name"`
	Email string    `json:"email"`
	Date  time.Time `json:"date"`
}

type ListCommitRequest struct {
	StartDate time.Time
	EndDate   time.Time
	Page      int
}
