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

// findBase returns the base of head under onto, for a land or a trial change that names none.
func (s *Server) findBase(ctx context.Context, repo store.Repo, onto, head string) (string, error) {
	o, err := gitpack.ParseSHA(onto)
	if err != nil {
		return "", store.ErrInvalid
	}
	h, err := gitpack.ParseSHA(head)
	if err != nil {
		return "", store.ErrInvalid
	}
	res := &reservation{take: func(n int64) (func(), error) { return s.objects.ReserveLand(ctx, n) }}
	defer res.drop()
	if err := res.growLand(0); err != nil {
		return "", err
	}
	rd, err := s.objects.Reader(ctx, repo)
	if err != nil {
		return "", err
	}
	base, err := land.FindBase(ctx, landSource{rd}, res.growLand, o, h)
	return base.String(), err
}

// answerLandError writes the answer for an error of a land that has a stored form, and returns the error otherwise.
func (s *Server) answerLandError(w http.ResponseWriter, err error) error {
	out, ok := outcomeOf(err)
	if !ok {
		return err
	}
	s.writeError(w, out.Code, out.Message, out.Details)
	return nil
}

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
		if !lowerHex(ch.Head, 40, 40) || ch.Base != "" && !lowerHex(ch.Base, 40, 40) {
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
	batch := land.NewBatch(landSource{reader}, res.growLand, tip, nil)
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
	var base gitpack.SHA
	var err error
	if ch.Base != "" {
		base, _ = gitpack.ParseSHA(ch.Base)
	} else {
		base, err = batch.FindBase(ctx, batch.Tip(), head)
	}
	var l land.Landed
	if err == nil {
		l, err = batch.LandExpecting(ctx, base, head, when, nil)
	}
	if err != nil {
		return s.trialFailure(err)
	}
	tree, err := batch.TreeOf(ctx, l.New)
	if err != nil {
		return api.TrialItem{}, err
	}
	item := api.TrialItem{Base: base.String(), Tree: tree.String(), Tip: l.New.String()}
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
