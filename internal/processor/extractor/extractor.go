package extractor

import (
	"context"
	"fmt"
	"github.com/chunguyenduc/git_commit_etl/internal/adapter/github"
	"github.com/chunguyenduc/git_commit_etl/internal/config"
	"github.com/chunguyenduc/git_commit_etl/internal/model"
	"github.com/chunguyenduc/git_commit_etl/internal/utils"
	"github.com/chunguyenduc/git_commit_etl/pkg/file"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

type Extractor struct {
	client     github.RepoClient
	cfg        *config.ExtractorConfig
	fileWriter *file.Writer
}

func New(cfg *config.ExtractorConfig) (*Extractor, error) {
	fw, err := file.NewFileWriter(cfg.StorageDir)
	if err != nil {
		return nil, err
	}
	return &Extractor{
		client:     github.NewRepoClient(cfg.SourceData),
		cfg:        cfg,
		fileWriter: fw,
	}, nil
}

func (e *Extractor) CollectCommitsByDate(ctx context.Context, startDate, endDate time.Time) (*model.ExtractedData, error) {
	var (
		mu   sync.Mutex
		once sync.Once
	)
	result := make([]*github.CommitResponse, 0)

	requestChan := make(chan *github.ListCommitRequest)
	doneChan := make(chan struct{})

	numWorker := e.cfg.IngestorWorker
	group, ctx := errgroup.WithContext(ctx)

	go func() {
		defer close(requestChan)
		for i := 1; ; i++ {
			select {
			case requestChan <- &github.ListCommitRequest{
				StartDate: startDate,
				EndDate:   endDate,
				Page:      i,
			}:
			case <-ctx.Done():
				return
			case <-doneChan:
				return
			}
		}
	}()

	workerFunc := func() error {
		for request := range requestChan {
			response, err := e.client.ListCommits(ctx, request)
			if err != nil {
				return err
			}

			if len(response) == 0 {
				once.Do(func() {
					close(doneChan)
				})
				return nil
			}

			mu.Lock()
			result = append(result, response...)
			mu.Unlock()
		}
		return nil
	}

	for i := 0; i < numWorker; i++ {
		group.Go(workerFunc)
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return &model.ExtractedData{
		Commits: result,
		Month:   int(startDate.Month()),
		Year:    startDate.Year(),
	}, nil
}

func (e *Extractor) Run(ctx context.Context) ([]string, error) {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(e.cfg.ExtractorWorker)

	fileNames := make([]string, 0, e.cfg.MonthCounts)
	var mu sync.RWMutex

	for i := 0; i < e.cfg.MonthCounts; i++ {
		startDate, endDate := buildStartEndDate(i)
		logger := log.Ctx(ctx).With().Str("start_date", utils.ToDateStr(startDate)).Str("end_date", utils.ToDateStr(endDate)).Logger()

		group.Go(func() error {
			logger.Info().Msg("Start fetching commits")
			extractedData, err := e.CollectCommitsByDate(ctx, startDate, endDate)
			if err != nil {
				return err
			}

			bytes, err := extractedData.Serialize()
			if err != nil {
				logger.Err(err).Msg("Error serializing extracted data")
				return err
			}

			fileName := fmt.Sprintf("commits-%d-%d.json", startDate.Month(), startDate.Year())
			if err = e.fileWriter.WriteFile(ctx, fileName, bytes); err != nil {
				return err
			}

			logger.Info().Int("num_commits", len(extractedData.Commits)).Msg("Saved commits to file successfully")

			mu.Lock()
			fileNames = append(fileNames, fileName)
			mu.Unlock()

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		log.Ctx(ctx).Err(err).Msg("Error running extractor")
		return nil, err
	}

	return fileNames, nil
}

func buildStartEndDate(i int) (time.Time, time.Time) {
	currentTime := time.Now()

	startTime := utils.AddMonth(currentTime, -i)
	endTime := utils.AddMonth(currentTime, -i+1)

	startDate := utils.StartOfMonth(startTime.Month(), startTime.Year())
	endDate := utils.StartOfMonth(endTime.Month(), endTime.Year())

	return startDate, endDate
}
