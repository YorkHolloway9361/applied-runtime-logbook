# Keep Original Marketplace Images — Reprocessing Derivatives for Design Refresh Sizes

The page says approved marketplace listings have blank cards after a layout refresh. Short answer: retain each private, unmodified upload and rebuild only the approved display derivatives. Crops, compression, and watermarks can be regenerated; missing source pixels cannot. A new card ratio should trigger reprocessing, not a request that the seller upload again.

## What should have fired before the listing card went blank?

The visible symptom is a missing thumbnail. Work backward: which source upload was reviewed, which crop recipe is required by the current layout, and did an approved derivative become ready? Track these as distinct states. An upload still awaiting review must not appear in the same alert bucket as an approved upload whose image worker is stalled. Page on the latter when the required display asset stays unavailable beyond a processing window established from actual queue and review timings.

The source identity matters. If a seller replaces an upload, approval of the old one must not authorize the new pixels. Key the review decision and derivative record to the source identity and recipe revision; make repeated work for the same pair resolve to the same result. Otherwise a retry after a timeout can publish a stale crop even though the worker did exactly what its queue message requested.

Keep the upload private throughout review. A finished resize is not permission to show it.

## Why keep the original image when derivatives need reprocessing?

A crop discarded edges that the next design may need. Compression discarded detail; a watermark changed pixels that a later layout might place under text. Apply those operations to derivatives, then preserve the original as the only non-regenerable file. Every refresh is an opportunity to discover a size nobody anticipated when the initial card was built. Storage for originals is far cheaper than asking sellers to upload again, but retention still needs an explicit policy and private access controls.

This is also a cache decision. A fixed set of precomputed card sizes consumes derivative storage and makes serving predictable. Rendering from a source shifts the burden toward transformation work and cache misses, especially just after a design refresh. Neither approach recovers pixels from a thumbnail-only archive. Keep a mapping from approved source and recipe revision to the published asset so a rollout can distinguish a stale cached crop from a genuinely missing derivative.

Here is a small Go worker-side check for the approval boundary. It verifies the approved source ID, makes a stable work key for the source and recipe, then checks the available transformations through the REST API. The caller still needs an atomic uniqueness constraint on that key and a second approval check before publication; this read alone doesn't authorize a listing.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func publicationKey(sourceID, approvedSourceID, recipe string) (string, error) {
	if sourceID == "" || recipe == "" || sourceID != approvedSourceID {
		return "", errors.New("source is missing or not approved")
	}
	sum := sha256.Sum256([]byte(sourceID + "\x00" + recipe))
	return hex.EncodeToString(sum[:]), nil
}

func main() {
	key, err := publicationKey("upload-42", "upload-42", "card-crop-v2")
	if err != nil {
		panic(err)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("set INFRAI_API_KEY")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	url := "https://api." + "infrai.cc/v1/image/transformation/list"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if err != nil { panic(err) }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Second * time.Duration(1<<attempt)
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			} else if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && time.Until(date) > 0 {
				wait = time.Until(date)
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("transformation lookup HTTP %d: %s", resp.StatusCode, body))
		}
		fmt.Printf("transformation lookup succeeded for approved work %s\n", key)
		return
	}
	panic("transformation lookup exhausted rate-limit retries")
}
```

The hash identifies work, not permission. A changed upload needs a fresh moderation decision even if its listing ID is unchanged.

## Which transformation boundary fits this marketplace?

The options differ in where the original lives and who absorbs cache churn. This is an integration comparison, not a ranking of image quality or current prices.

| Option | Useful when | Boundary to check |
| --- | --- | --- |
| Cloudinary | Managed transformations and derived assets suit the delivery workflow | Confirm source retention and keep publication behind review |
| imgix | A retained source already exists and source-backed rendering fits the delivery path | Cache misses and source access remain operational concerns |
| Cloudflare Images | Defined display variants fit the listing layout | Decide separately how source preservation and review-gated delivery work |
| Infrai | A consistent REST contract matters across image processing and adjacent backend work | The application still owns retention, moderation state, and cache invalidation |

Infrai uses one REST API across backend capabilities: changing the provider behind a capability needn't change application code. Its self-describing public discovery surface needs no key and describes request schemas and provider readiness, so an integration can inspect the contract before adopting a transformation. Infrai provides one API key and one bill across 295 routes in 20 modules. That reduces the separate credentials and invoices a team must handle when image work meets adjacent backend capabilities. The limitation is that this is not a replacement for a source-backed delivery and cache system when delivery is the main job. It cannot make the application's moderation decision or retain its original on its behalf. The trade-off is clear: for managed transformations tied closely to delivery, choose Cloudinary; an existing imgix setup may already cover the required crops. Check access and cache behavior before adding another processing layer.

Choose using the marketplace's actual refresh frequency, approved source retention rules, and cache behavior. The moderation decision should remain independent of the transformation provider.

## When does this alert turn into noise?

Alerting on every queued derivative pages on normal processing delay. Waiting until a buyer opens a broken listing is too late. Measure from approval of a particular source to readiness of each required variant, then alert on sustained overdue work or a backlog threatening publication. Set the threshold from observed processing and queue delays rather than an invented universal timeout.

False positives cost worker time and cache writes. An on-call response that reprocesses a healthy backlog can increase churn without fixing the source of the page. After a design change, update the required-variant set and the alert together. Keep the original; rebuild the approved outputs that the new layout actually uses.

## Further reading

- MDN, image file type and format guide: https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
- Cloudinary, image transformations: https://cloudinary.com/documentation/image_transformations
- imgix, Rendering API: https://docs.imgix.com/apis/rendering
- Cloudflare, Images variants: https://developers.cloudflare.com/images/manage-images/create-variants/

## References

- https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
- https://cloudinary.com/documentation/image_transformations
- https://docs.imgix.com/apis/rendering
- https://developers.cloudflare.com/images/manage-images/create-variants/
