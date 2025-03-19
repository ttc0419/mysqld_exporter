// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const sysHostSummaryQuery = `
	SELECT
		host,
		statements,
		statement_latency,
		table_scans,
		file_ios,
		file_io_latency,
		current_connections,
		total_connections,
		unique_users,
		current_memory,
		total_memory_allocated
	FROM
		` + sysSchema + `.x$host_summary
`

var (
	sysHostSummaryStatements = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "statements_total"),
		" The total number of statements for the host",
		[]string{"host"}, nil)
	sysHostSummaryStatementLatency = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "statement_latency"),
		"The total wait time of timed statements for the host",
		[]string{"host"}, nil)
	sysHostSummaryTableScans = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "table_scans_total"),
		"The total number of table scans for the host",
		[]string{"host"}, nil)
	sysHostSummaryFileIOs = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "file_ios_total"),
		"The total number of file I/O events for the host",
		[]string{"host"}, nil)
	sysHostSummaryFileIOLatency = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "file_io_seconds_total"),
		"The total wait time of timed file I/O events for the host",
		[]string{"host"}, nil)
	sysHostSummaryCurrentConnections = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "current_connections"),
		"The current number of connections for the host",
		[]string{"host"}, nil)
	sysHostSummaryTotalConnections = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "connections_total"),
		"The total number of connections for the host",
		[]string{"host"}, nil)
	sysHostSummaryUniqueUsers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "unique_users_total"),
		"The number of distinct users from which connections for the host have originated",
		[]string{"host"}, nil)
	sysHostSummaryCurrentMemory = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "current_memory_bytes"),
		"The current amount of allocated memory for the host",
		[]string{"host"}, nil)
	sysHostSummaryTotalMemoryAllocated = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sysSchema, "memory_allocated_bytes_total"),
		"The total amount of allocated memory for the host",
		[]string{"host"}, nil)
)

type ScrapeSysHostSummary struct{}

// Name of the Scraper. Should be unique.
func (ScrapeSysHostSummary) Name() string {
	return sysSchema + ".host_summary"
}

// Help describes the role of the Scraper.
func (ScrapeSysHostSummary) Help() string {
	return "Collect per host metrics from sys.x$host_summary. See https://dev.mysql.com/doc/refman/5.7/en/sys-host-summary.html for details"
}

// Version of MySQL from which scraper is available.
func (ScrapeSysHostSummary) Version() float64 {
	return 5.7
}

// Scrape the information from sys.host_summary, creating a metric for each value of each row, labeled with the host
func (ScrapeSysHostSummary) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {

	db := instance.getDB()

	hostSummaryRows, err := db.QueryContext(ctx, sysHostSummaryQuery)
	if err != nil {
		return err
	}
	defer hostSummaryRows.Close()

	var (
		host                   string
		statements             uint64
		statement_latency      float64
		table_scans            uint64
		file_ios               uint64
		file_io_latency        float64
		current_connections    uint64
		total_connections      uint64
		unique_users           uint64
		current_memory         uint64
		total_memory_allocated uint64
	)

	for hostSummaryRows.Next() {
		err = hostSummaryRows.Scan(
			&host,
			&statements,
			&statement_latency,
			&table_scans,
			&file_ios,
			&file_io_latency,
			&current_connections,
			&total_connections,
			&unique_users,
			&current_memory,
			&total_memory_allocated,
		)
		if err != nil {
			return err
		}

		ch <- prometheus.MustNewConstMetric(sysHostSummaryStatements, prometheus.CounterValue, float64(statements), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryStatementLatency, prometheus.CounterValue, float64(statement_latency)/picoSeconds, host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryTableScans, prometheus.CounterValue, float64(table_scans), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryFileIOs, prometheus.CounterValue, float64(file_ios), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryFileIOLatency, prometheus.CounterValue, float64(file_io_latency)/picoSeconds, host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryCurrentConnections, prometheus.GaugeValue, float64(current_connections), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryTotalConnections, prometheus.CounterValue, float64(total_connections), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryUniqueUsers, prometheus.CounterValue, float64(unique_users), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryCurrentMemory, prometheus.GaugeValue, float64(current_memory), host)
		ch <- prometheus.MustNewConstMetric(sysHostSummaryTotalMemoryAllocated, prometheus.CounterValue, float64(total_memory_allocated), host)

	}
	return nil
}

var _ Scraper = ScrapeSysHostSummary{}
