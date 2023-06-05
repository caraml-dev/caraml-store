package serving

import (
	"fmt"
	servingproto "github.com/caraml-dev/caraml-store/caraml-store-sdk/go/protos/feast/serving"
	"strings"
)

var (
	// ErrInvalidFeatureRef indicates that the user has provided a feature reference
	// with the wrong structure or contents
	ErrInvalidFeatureRef = "Invalid Feature Reference %s provided, " +
		"feature reference must be in the format featureTableName:featureName"
)

// OnlineFeaturesRequest wrapper on feast.serving.GetOnlineFeaturesRequestV2.
type OnlineFeaturesRequest struct {
	// Features is the list of features to obtain from Feast. Each feature can be given as
	// the format feature_table:feature, where "feature_table" & "feature" are feature table name
	// and feature name respectively. The only required components is feature name.
	Features []string

	// Entities is the list of entity rows to retrieve features on. Each row is a map of entity name to entity value.
	Entities []Row

	// Project optionally specifies the project override. If specified, uses given project for retrieval.
	// Overrides the projects specified in Feature References if also specified.
	Project string
}

// Builds the feast-specified request payload from the wrapper.
func (r OnlineFeaturesRequest) buildRequest() (*servingproto.GetOnlineFeaturesRequest, error) {
	featureRefs, err := buildFeatureRefs(r.Features)
	if err != nil {
		return nil, err
	}

	// build request entity rows from native entities
	entityRows := make([]*servingproto.GetOnlineFeaturesRequest_EntityRow, len(r.Entities))
	for i, entity := range r.Entities {
		entityRows[i] = &servingproto.GetOnlineFeaturesRequest_EntityRow{
			Fields: entity,
		}
	}

	return &servingproto.GetOnlineFeaturesRequest{
		Features:   featureRefs,
		EntityRows: entityRows,
		Project:    r.Project,
	}, nil
}

// Creates a slice of FeatureReferences from string representation in
// the format featuretable:feature.
// featureRefStrs - string feature references to parse.
// Returns parsed FeatureReferences.
// Returns an error when the format of the string feature reference is invalid
func buildFeatureRefs(featureRefStrs []string) ([]*servingproto.FeatureReference, error) {
	var featureRefs []*servingproto.FeatureReference

	for _, featureRefStr := range featureRefStrs {
		featureRef, err := parseFeatureRef(featureRefStr)
		if err != nil {
			return nil, err
		}
		featureRefs = append(featureRefs, featureRef)
	}
	return featureRefs, nil
}

// Parses a string FeatureReference into FeatureReference proto
// featureRefStr - the string feature reference to parse.
// Returns parsed FeatureReference.
// Returns an error when the format of the string feature reference is invalid
func parseFeatureRef(featureRefStr string) (*servingproto.FeatureReference, error) {
	if len(featureRefStr) == 0 {
		return nil, fmt.Errorf(ErrInvalidFeatureRef, featureRefStr)
	}

	var featureRef servingproto.FeatureReference
	if strings.Contains(featureRefStr, "/") || !strings.Contains(featureRefStr, ":") {
		return nil, fmt.Errorf(ErrInvalidFeatureRef, featureRefStr)
	}
	// parse featuretable if specified
	if strings.Contains(featureRefStr, ":") {
		refSplit := strings.Split(featureRefStr, ":")
		featureRef.FeatureTable, featureRefStr = refSplit[0], refSplit[1]
	}
	featureRef.Name = featureRefStr

	return &featureRef, nil
}
