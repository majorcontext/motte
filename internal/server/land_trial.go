package server

import (
	"context"
	"net/http"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/land"
	"github.com/majorcontext/motte/internal/store"
)

// landTrial lands the changes one after the other on the tip of the request, and writes the objects that they make. It
// moves no ref and writes no queue row, log row or event.
func (s *Server) landTrial(w http.ResponseWriter, r *http.Request, c call) error {
	var req api.TrialRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if r.PathValue("repo") != c.claims.Repo {
		return errForbidden
	}
	if !lowerHex(req.Tip, 40, 40) || len(req.Changes) == 0 || len(req.Changes) > s.lands.MaxBatch {
		return errBody
	}
	for _, ch := range req.Changes {
		if !lowerHex(ch.Head, 40, 40) || !lowerHex(ch.Base, 40, 40) {
			return errBody
		}
	}
	tip, _ := gitpack.ParseSHA(req.Tip)
	ctx, _ := store.WithStats(r.Context())
	res := &reservation{take: func(n int64) (func(), error) { return s.objects.ReserveLand(ctx, n) }}
	defer res.drop()
	if err := res.growLand(0); err != nil {
		return err
	}
	reader, err := s.objects.Reader(ctx, c.repo)
	if err != nil {
		return err
	}
	batch := land.NewBatch(landSource{reader}, res.growLand, tip, nil, land.Options{})
	when := s.events.Clock.Now()
	out := api.TrialResponse{Items: make([]api.TrialItem, 0, len(req.Changes))}
	for _, ch := range req.Changes {
		item, err := s.trialStep(ctx, batch, ch, when)
		if err != nil {
			return err
		}
		out.Items = append(out.Items, item)
	}
	if objs := batch.Objects(); len(objs) > 0 {
		if err := res.growLand(int64(batch.Bytes())); err != nil {
			return err
		}
		for _, part := range splitObjects(objs, store.MaxBatch/2, store.MaxPackBytes/2) {
			if _, err := s.objects.PutObjects(ctx, c.repo, part); err != nil {
				return err
			}
		}
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) trialStep(ctx context.Context, batch *land.Batch, ch api.TrialChange, when time.Time) (api.TrialItem, error) {
	if batch.Bytes() >= softBatchBytes {
		return api.TrialItem{Error: &api.Error{Code: api.CodeObjectTooLarge, Message: "the trial holds as many new objects as one call keeps: send the remaining changes in another call", Details: map[string]any{}}}, nil
	}
	head, _ := gitpack.ParseSHA(ch.Head)
	base, _ := gitpack.ParseSHA(ch.Base)
	l, err := batch.LandExpecting(ctx, base, head, when, nil)
	if err != nil {
		return s.trialFailure(err)
	}
	tree, err := batch.TreeOf(ctx, l.New)
	if err != nil {
		return api.TrialItem{}, err
	}
	item := api.TrialItem{Base: base.String(), Tree: tree.String(), Tip: l.New.String(), Commits: []api.LandedCommit{}}
	for _, m := range l.Commits {
		item.Commits = append(item.Commits, api.LandedCommit{Old: m.Old.String(), New: m.New.String()})
	}
	return item, nil
}

// trialFailure turns the error of one change into its item, and returns an error that no item can stand for.
func (s *Server) trialFailure(err error) (api.TrialItem, error) {
	out, ok := outcomeOf(err)
	if !ok {
		return api.TrialItem{}, err
	}
	if out.Code == api.CodeLandConflict {
		paths, _ := out.Details["paths"].([]string)
		return api.TrialItem{Conflict: &api.TrialConflict{Paths: paths}}, nil
	}
	return api.TrialItem{Error: &api.Error{Code: out.Code, Message: out.Message, Details: out.Details}}, nil
}
