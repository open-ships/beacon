package api

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/open-ships/beacon/internal/n2kcatalog"
)

type catalogOutput struct {
	Body struct {
		PGNs []n2kcatalog.PGN `json:"pgns" doc:"Known NMEA 2000 PGNs and every decoded variant."`
	}
}

type catalogPGNInput struct {
	PGN uint32 `path:"pgn" doc:"NMEA 2000 Parameter Group Number."`
}

type catalogPGNOutput struct{ Body n2kcatalog.PGN }

func registerCatalogRoutes(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "list-n2k-pgns", Method: http.MethodGet,
		Path: "/api/v1/n2k/pgns", Summary: "List the runtime NMEA 2000 PGN and field catalog",
	}, func(ctx context.Context, _ *struct{}) (*catalogOutput, error) {
		out := &catalogOutput{}
		out.Body.PGNs = n2kcatalog.List()
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "get-n2k-pgn", Method: http.MethodGet,
		Path: "/api/v1/n2k/pgns/{pgn}", Summary: "Get one NMEA 2000 PGN and its variants",
		Errors: []int{http.StatusNotFound},
	}, func(ctx context.Context, in *catalogPGNInput) (*catalogPGNOutput, error) {
		item, ok := n2kcatalog.Lookup(in.PGN)
		if !ok {
			return nil, huma.Error404NotFound("PGN not found")
		}
		return &catalogPGNOutput{Body: item}, nil
	})
}
