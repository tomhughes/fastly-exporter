package rt

import (
	"strconv"

	"github.com/peterbourgon/fastly-exporter/pkg/prom"
	"github.com/prometheus/client_golang/prometheus"
)

// process the data from the realtime response, and feed the interpreted results
// to the Prometheus metrics as observations.
func process(rt RealtimeResponse, serviceID, serviceName, serviceVersion string, m *prom.Metrics) {
	for _, d := range rt.Data {
		for datacenter, stats := range d.Datacenter {
			m.ServiceInfo.WithLabelValues(serviceID, serviceName, serviceVersion).Set(1)
			m.RequestsTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Requests))

			m.AttackBlockedReqBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackBlockedReqBodyBytes))
			m.AttackBlockedReqHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackBlockedReqHeaderBytes))
			m.AttackLoggedReqBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackLoggedReqBodyBytes))
			m.AttackLoggedReqHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackLoggedReqHeaderBytes))
			m.AttackPassedReqBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackPassedReqBodyBytes))
			m.AttackPassedReqHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackPassedReqHeaderBytes))
			m.AttackReqBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackReqBodyBytes))
			m.AttackReqHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackReqHeaderBytes))
			m.AttackRespSynthBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.AttackRespSynthBytes))
			m.BackendReqBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.BackendReqBodyBytes))
			m.BackendReqHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.BackendReqHeaderBytes))
			m.Billed.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Billed))
			m.BilledBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.BilledBodyBytes))
			m.BilledHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.BilledHeaderBytes))
			m.Blacklisted.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Blacklisted))
			m.BodySizeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.BodySize))
			m.DeliverSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.DeliverSubCount))
			m.DeliverSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.DeliverSubTime))
			m.Edge.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Edge))
			m.EdgeRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.EdgeRespBodyBytes))
			m.EdgeRespHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.EdgeRespHeaderBytes))
			m.ErrorsTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Errors))
			m.ErrorSubCount.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ErrorSubCount))
			m.ErrorSubTime.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ErrorSubTime))
			m.FetchSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.FetchSubCount))
			m.FetchSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.FetchSubTime))
			m.HashSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HashSubCount))
			m.HashSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HashSubTime))
			m.HeaderSizeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HeaderSize))
			m.HitRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HitRespBodyBytes))
			m.HitsTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HitsTime))
			m.HitsTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Hits))
			m.HitSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HitSubCount))
			m.HitSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HitSubTime))
			m.HTTP2Total.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.HTTP2))
			m.ImgOptoRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoRespBodyBytes))
			m.ImgOptoRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoRespHeaderBytes))
			m.ImgOptoShieldRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoShieldRespBodyBytes))
			m.ImgOptoShieldRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoShieldRespHeaderBytes))
			m.ImgOptoShieldTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoShield))
			m.ImgOptoTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOpto))
			m.ImgOptoTransformRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoTransformRespBodyBytes))
			m.ImgOptoTransformRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoTransformRespHeaderBytes))
			m.ImgOptoTransformTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgOptoTransform))
			m.ImgVideo.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideo))
			m.ImgVideoFrames.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoFrames))
			m.ImgVideoRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoRespBodyBytes))
			m.ImgVideoRespHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoRespHeaderBytes))
			m.ImgVideoShield.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoShield))
			m.ImgVideoShieldFrames.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoShieldFrames))
			m.ImgVideoShieldRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoShieldRespBodyBytes))
			m.ImgVideoShieldRespHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ImgVideoShieldRespHeaderBytes))
			m.IPv6Total.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.IPv6))
			m.LogBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.LogBytes))
			m.LoggingTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Logging))
			m.MissesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Misses))
			m.MissRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.MissRespBodyBytes))
			m.MissSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.MissSubCount))
			m.MissSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.MissSubTime))
			m.MissTime.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.MissTime))
			m.MissTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.MissTime))
			m.ObjectSizeOther.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ObjectSizeOther))
			m.OriginFetchBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OriginFetchBodyBytes))
			m.OriginFetches.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OriginFetches))
			m.OriginFetchHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OriginFetchHeaderBytes))
			m.OriginFetchRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OriginFetchRespBodyBytes))
			m.OriginFetchRespHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OriginFetchRespHeaderBytes))
			m.OriginRevalidations.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OriginRevalidations))
			m.OTFPDeliverTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPDeliverTime))
			m.OTFPManifestTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPManifest))
			m.OTFPRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPRespBodyBytes))
			m.OTFPRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPRespHeaderBytes))
			m.OTFPShieldRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPShieldRespBodyBytes))
			m.OTFPShieldRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPShieldRespHeaderBytes))
			m.OTFPShieldTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPShieldTime))
			m.OTFPShieldTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPShield))
			m.OTFPTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFP))
			m.OTFPTransformRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPTransformRespBodyBytes))
			m.OTFPTransformRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPTransformRespHeaderBytes))
			m.OTFPTransformTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPTransformTime))
			m.OTFPTransformTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.OTFPTransform))
			m.PassesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Passes))
			m.PassRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PassRespBodyBytes))
			m.PassSubCount.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PassSubCount))
			m.PassSubTime.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PassSubTime))
			m.PassTime.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PassTime))
			m.PassTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PassTime))
			m.PCITotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PCI))
			m.PipeSubCount.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PipeSubCount))
			m.PipeSubTime.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PipeSubTime))
			m.PredeliverSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PredeliverSubCount))
			m.PredeliverSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PredeliverSubTime))
			m.PrehashSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PrehashSubCount))
			m.PrehashSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.PrehashSubTime))
			m.RecvSubCountTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.RecvSubCount))
			m.RecvSubTimeTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.RecvSubTime))
			m.ReqBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ReqBodyBytes))
			m.ReqHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ReqHeaderBytes))
			m.RespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.RespBodyBytes))
			m.RespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.RespHeaderBytes))
			m.Restart.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Restart))
			m.SegBlockOriginFetches.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.SegBlockOriginFetches))
			m.SegBlockShieldFetches.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.SegBlockShieldFetches))
			m.ShieldFetchBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldFetchBodyBytes))
			m.ShieldFetches.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldFetches))
			m.ShieldFetchHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldFetchHeaderBytes))
			m.ShieldFetchRespBodyBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldFetchRespBodyBytes))
			m.ShieldFetchRespHeaderBytes.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldFetchRespHeaderBytes))
			m.ShieldRespBodyBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldRespBodyBytes))
			m.ShieldRespHeaderBytesTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldRespHeaderBytes))
			m.ShieldRevalidations.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.ShieldRevalidations))
			m.ShieldTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Shield))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "200").Add(float64(stats.Status200))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "204").Add(float64(stats.Status204))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "206").Add(float64(stats.Status206))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "301").Add(float64(stats.Status301))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "302").Add(float64(stats.Status302))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "304").Add(float64(stats.Status304))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "400").Add(float64(stats.Status400))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "401").Add(float64(stats.Status401))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "403").Add(float64(stats.Status403))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "404").Add(float64(stats.Status404))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "416").Add(float64(stats.Status416))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "429").Add(float64(stats.Status429))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "500").Add(float64(stats.Status500))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "501").Add(float64(stats.Status501))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "502").Add(float64(stats.Status502))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "503").Add(float64(stats.Status503))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "504").Add(float64(stats.Status504))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "505").Add(float64(stats.Status505))
			m.StatusCodeTotal.WithLabelValues(serviceID, serviceName, datacenter, "505").Add(float64(stats.Status505))
			m.StatusGroupTotal.WithLabelValues(serviceID, serviceName, datacenter, "1xx").Add(float64(stats.Status1xx))
			m.StatusGroupTotal.WithLabelValues(serviceID, serviceName, datacenter, "2xx").Add(float64(stats.Status2xx))
			m.StatusGroupTotal.WithLabelValues(serviceID, serviceName, datacenter, "3xx").Add(float64(stats.Status3xx))
			m.StatusGroupTotal.WithLabelValues(serviceID, serviceName, datacenter, "4xx").Add(float64(stats.Status4xx))
			m.StatusGroupTotal.WithLabelValues(serviceID, serviceName, datacenter, "5xx").Add(float64(stats.Status5xx))
			m.SynthsTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Synths))
			m.TLSTotal.WithLabelValues(serviceID, serviceName, datacenter, "any").Add(float64(stats.TLS))
			m.TLSTotal.WithLabelValues(serviceID, serviceName, datacenter, "v10").Add(float64(stats.TLSv10))
			m.TLSTotal.WithLabelValues(serviceID, serviceName, datacenter, "v11").Add(float64(stats.TLSv11))
			m.TLSTotal.WithLabelValues(serviceID, serviceName, datacenter, "v12").Add(float64(stats.TLSv12))
			m.TLSTotal.WithLabelValues(serviceID, serviceName, datacenter, "v13").Add(float64(stats.TLSv13))
			m.UncacheableTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Uncacheable))
			m.VideoTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.Video))
			m.WAFBlockedTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.WAFBlocked))
			m.WAFLoggedTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.WAFLogged))
			m.WAFPassedTotal.WithLabelValues(serviceID, serviceName, datacenter).Add(float64(stats.WAFPassed))

			processHistogram(stats.MissHistogram, m.MissDurationSeconds.WithLabelValues(serviceID, serviceName, datacenter))
			processObjectSizes(
				stats.ObjectSize1k, stats.ObjectSize10k, stats.ObjectSize100k,
				stats.ObjectSize1m, stats.ObjectSize10m, stats.ObjectSize100m,
				stats.ObjectSize1g, m.ObjectSizeBytes.WithLabelValues(serviceID, serviceName, datacenter),
			)

		}
	}
}

func processHistogram(src map[string]uint64, obs prometheus.Observer) {
	for str, count := range src {
		ms, err := strconv.Atoi(str)
		if err != nil {
			continue
		}
		s := float64(ms) / 1e3
		for i := 0; i < int(count); i++ {
			obs.Observe(s)
		}
	}
}

func processObjectSizes(n1k, n10k, n100k, n1m, n10m, n100m, n1g uint64, obs prometheus.Observer) {
	for v, n := range map[uint64]uint64{
		1 * 1024:           n1k,
		10 * 1024:          n10k,
		100 * 1024:         n100k,
		1 * 1000 * 1024:    n1m,
		10 * 1000 * 1024:   n10m,
		100 * 1000 * 1024:  n100m,
		1000 * 1000 * 1024: n1g,
	} {
		for i := uint64(0); i < n; i++ {
			obs.Observe(float64(v))
		}
	}
}
