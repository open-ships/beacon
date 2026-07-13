package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/open-ships/beacon/internal/config"
	"github.com/open-ships/beacon/internal/model"
	"github.com/open-ships/beacon/internal/supervisor"
)

// StatusBody is the response body for every write endpoint (PUT, DELETE):
// the subset of the supervisor's live component statuses that pertain to
// the entity just written, letting a caller see the effect of a hot apply
// without a second round trip to a statuses endpoint.
type StatusBody struct {
	Status []supervisor.Status `json:"status" doc:"Supervisor statuses for the affected entity (filtered to its id)."`
}

// writeOutput is the response envelope shared by every PUT/DELETE
// operation below.
type writeOutput struct {
	Body StatusBody
}

// idInput is the path-parameter shape shared by every get-one/put/delete
// operation below.
type idInput struct {
	ID string `path:"id" doc:"Entity id."`
}

// filterStatuses returns the subset of all whose ID matches id, preserving
// order. Used to scope a write's response to the entity that was written
// rather than dumping every component's status on every write.
func filterStatuses(all []supervisor.Status, id string) []supervisor.Status {
	out := make([]supervisor.Status, 0, 1)
	for _, s := range all {
		if s.ID == id {
			out = append(out, s)
		}
	}
	return out
}

// mapServiceErr converts an error returned by internal/config.Service into
// the huma.StatusError the HTTP layer should return:
//
//   - *config.ValidationError (structural or CEL-compile problem) -> 422
//   - config.ErrNotFound                                          -> 404
//   - config.ErrExists                                            -> 409
//   - config.ErrInUse                                             -> 409
//
// ErrExists is not expected on the PUT path in ordinary operation (PUT
// decides isCreate from a Get immediately beforehand — see putXxx below),
// but stays mapped here in case a racing writer creates the entity between
// that Get and this PUT's Service call; the client sees a 409 it can retry
// rather than a mysterious 500. Any other error (store/IO failure) is
// returned as-is, which huma reports as a 500.
func mapServiceErr(err error) error {
	var ve *config.ValidationError
	switch {
	case errors.As(err, &ve):
		return huma.Error422UnprocessableEntity(ve.Msg)
	case errors.Is(err, config.ErrNotFound):
		return huma.Error404NotFound(err.Error())
	case errors.Is(err, config.ErrExists):
		return huma.Error409Conflict(err.Error())
	case errors.Is(err, config.ErrInUse):
		return huma.Error409Conflict(err.Error())
	default:
		return err
	}
}

// --- Sources ---

type listSourcesOutput struct {
	Body struct {
		Sources []model.Source `json:"sources" doc:"All configured sources."`
	}
}

type getSourceOutput struct {
	Body model.Source
}

type putSourceInput struct {
	ID   string       `path:"id" doc:"Source id."`
	Body model.Source `doc:"Source definition; body id must equal the path id."`
}

func registerSourceRoutes(api huma.API, svc *config.Service) {
	huma.Register(api, huma.Operation{
		OperationID: "list-sources",
		Method:      http.MethodGet,
		Path:        "/api/v1/sources",
		Summary:     "List sources",
	}, func(ctx context.Context, _ *struct{}) (*listSourcesOutput, error) {
		sources, err := svc.ListSources(ctx)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		out := &listSourcesOutput{}
		out.Body.Sources = sources
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "get-source",
		Method:      http.MethodGet,
		Path:        "/api/v1/sources/{id}",
		Summary:     "Get a source",
		Errors:      []int{http.StatusNotFound},
	}, func(ctx context.Context, in *idInput) (*getSourceOutput, error) {
		source, err := svc.GetSource(ctx, in.ID)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		return &getSourceOutput{Body: source}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "put-source",
		Method:      http.MethodPut,
		Path:        "/api/v1/sources/{id}",
		Summary:     "Create or update a source",
		Errors:      []int{http.StatusUnprocessableEntity, http.StatusConflict},
	}, func(ctx context.Context, in *putSourceInput) (*writeOutput, error) {
		if in.Body.ID != in.ID {
			return nil, huma.Error422UnprocessableEntity(
				fmt.Sprintf("body id %q does not match path id %q", in.Body.ID, in.ID))
		}
		isCreate, err := isCreateSource(ctx, svc, in.ID)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		if err := svc.PutSource(ctx, in.Body, isCreate); err != nil {
			return nil, mapServiceErr(err)
		}
		return writeStatus(svc, in.ID), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "delete-source",
		Method:      http.MethodDelete,
		Path:        "/api/v1/sources/{id}",
		Summary:     "Delete a source",
		Errors:      []int{http.StatusNotFound, http.StatusConflict},
	}, func(ctx context.Context, in *idInput) (*writeOutput, error) {
		if err := svc.DeleteSource(ctx, in.ID); err != nil {
			return nil, mapServiceErr(err)
		}
		return writeStatus(svc, in.ID), nil
	})
}

// isCreateSource decides the isCreate flag PutSource needs for
// create-or-update PUT semantics: Service.PutSource itself takes an
// explicit isCreate bool (isCreate=true fails with ErrExists if the id
// already exists; isCreate=false fails with ErrNotFound if it doesn't), so
// a create-or-update caller must know which case it is in before calling
// Put. We decide by a Get immediately beforehand.
//
// This is a plain read-then-write, not a check-then-act under one lock:
// another writer can create or delete the same id between this Get and the
// Put call below. Service.PutSource is not extended with its own
// create-or-update mode for this because the race is benign — the Put call
// still validates and still enforces its own isCreate invariant, so a
// racing writer just turns this request into an ErrExists or ErrNotFound
// that mapServiceErr reports as 409/404 for the client to retry. Nothing
// is left inconsistent.
func isCreateSource(ctx context.Context, svc *config.Service, id string) (bool, error) {
	_, err := svc.GetSource(ctx, id)
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, config.ErrNotFound):
		return true, nil
	default:
		return false, err
	}
}

// --- Sinks ---

type listSinksOutput struct {
	Body struct {
		Sinks []model.Sink `json:"sinks" doc:"All configured sinks."`
	}
}

type getSinkOutput struct {
	Body model.Sink
}

type putSinkInput struct {
	ID   string     `path:"id" doc:"Sink id."`
	Body model.Sink `doc:"Sink definition; body id must equal the path id."`
}

func registerSinkRoutes(api huma.API, svc *config.Service) {
	huma.Register(api, huma.Operation{
		OperationID: "list-sinks",
		Method:      http.MethodGet,
		Path:        "/api/v1/sinks",
		Summary:     "List sinks",
	}, func(ctx context.Context, _ *struct{}) (*listSinksOutput, error) {
		sinks, err := svc.ListSinks(ctx)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		out := &listSinksOutput{}
		out.Body.Sinks = sinks
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "get-sink",
		Method:      http.MethodGet,
		Path:        "/api/v1/sinks/{id}",
		Summary:     "Get a sink",
		Errors:      []int{http.StatusNotFound},
	}, func(ctx context.Context, in *idInput) (*getSinkOutput, error) {
		sink, err := svc.GetSink(ctx, in.ID)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		return &getSinkOutput{Body: sink}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "put-sink",
		Method:      http.MethodPut,
		Path:        "/api/v1/sinks/{id}",
		Summary:     "Create or update a sink",
		Errors:      []int{http.StatusUnprocessableEntity, http.StatusConflict},
	}, func(ctx context.Context, in *putSinkInput) (*writeOutput, error) {
		if in.Body.ID != in.ID {
			return nil, huma.Error422UnprocessableEntity(
				fmt.Sprintf("body id %q does not match path id %q", in.Body.ID, in.ID))
		}
		isCreate, err := isCreateSink(ctx, svc, in.ID)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		if err := svc.PutSink(ctx, in.Body, isCreate); err != nil {
			return nil, mapServiceErr(err)
		}
		return writeStatus(svc, in.ID), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "delete-sink",
		Method:      http.MethodDelete,
		Path:        "/api/v1/sinks/{id}",
		Summary:     "Delete a sink",
		Errors:      []int{http.StatusNotFound, http.StatusConflict},
	}, func(ctx context.Context, in *idInput) (*writeOutput, error) {
		if err := svc.DeleteSink(ctx, in.ID); err != nil {
			return nil, mapServiceErr(err)
		}
		return writeStatus(svc, in.ID), nil
	})
}

// isCreateSink mirrors isCreateSource for sinks; see its doc comment for
// why a Get-then-Put race is acceptable here.
func isCreateSink(ctx context.Context, svc *config.Service, id string) (bool, error) {
	_, err := svc.GetSink(ctx, id)
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, config.ErrNotFound):
		return true, nil
	default:
		return false, err
	}
}

// --- Connectors ---

type listConnectorsOutput struct {
	Body struct {
		Connectors []model.Connector `json:"connectors" doc:"All configured connectors."`
	}
}

type getConnectorOutput struct {
	Body model.Connector
}

type putConnectorInput struct {
	ID   string          `path:"id" doc:"Connector id."`
	Body model.Connector `doc:"Connector definition; body id must equal the path id."`
}

func registerConnectorRoutes(api huma.API, svc *config.Service) {
	huma.Register(api, huma.Operation{
		OperationID: "list-connectors",
		Method:      http.MethodGet,
		Path:        "/api/v1/connectors",
		Summary:     "List connectors",
	}, func(ctx context.Context, _ *struct{}) (*listConnectorsOutput, error) {
		connectors, err := svc.ListConnectors(ctx)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		out := &listConnectorsOutput{}
		out.Body.Connectors = connectors
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "get-connector",
		Method:      http.MethodGet,
		Path:        "/api/v1/connectors/{id}",
		Summary:     "Get a connector",
		Errors:      []int{http.StatusNotFound},
	}, func(ctx context.Context, in *idInput) (*getConnectorOutput, error) {
		connector, err := svc.GetConnector(ctx, in.ID)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		return &getConnectorOutput{Body: connector}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "put-connector",
		Method:      http.MethodPut,
		Path:        "/api/v1/connectors/{id}",
		Summary:     "Create or update a connector",
		Errors:      []int{http.StatusUnprocessableEntity, http.StatusConflict},
	}, func(ctx context.Context, in *putConnectorInput) (*writeOutput, error) {
		if in.Body.ID != in.ID {
			return nil, huma.Error422UnprocessableEntity(
				fmt.Sprintf("body id %q does not match path id %q", in.Body.ID, in.ID))
		}
		isCreate, err := isCreateConnector(ctx, svc, in.ID)
		if err != nil {
			return nil, mapServiceErr(err)
		}
		if err := svc.PutConnector(ctx, in.Body, isCreate); err != nil {
			return nil, mapServiceErr(err)
		}
		return writeStatus(svc, in.ID), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "delete-connector",
		Method:      http.MethodDelete,
		Path:        "/api/v1/connectors/{id}",
		Summary:     "Delete a connector",
		Errors:      []int{http.StatusNotFound},
	}, func(ctx context.Context, in *idInput) (*writeOutput, error) {
		if err := svc.DeleteConnector(ctx, in.ID); err != nil {
			return nil, mapServiceErr(err)
		}
		return writeStatus(svc, in.ID), nil
	})
}

// isCreateConnector mirrors isCreateSource for connectors; see its doc
// comment for why a Get-then-Put race is acceptable here.
func isCreateConnector(ctx context.Context, svc *config.Service, id string) (bool, error) {
	_, err := svc.GetConnector(ctx, id)
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, config.ErrNotFound):
		return true, nil
	default:
		return false, err
	}
}

// writeStatus builds a writeOutput scoped to id from the reconciler's
// current statuses, for use as the response of every PUT/DELETE handler
// above.
func writeStatus(svc *config.Service, id string) *writeOutput {
	out := &writeOutput{}
	out.Body.Status = filterStatuses(svc.Statuses(), id)
	return out
}
