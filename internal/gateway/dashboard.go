package gateway

import _ "embed"

// dashboardHTML is the single-page console, served at / from the same origin
// as the /api endpoints it calls.
//
//go:embed dashboard.html
var dashboardHTML string
