package asyncworker

import "encoding/json"

// ParseUsage extracts the OpenAI usage object from a response body. ok is
// false when the body is not valid JSON or carries no usage object; such
// bodies are silently ignored (best-effort parse on a hot path). Negative
// token counts are clamped to 0 so they can never panic a counter Add.
func ParseUsage(body []byte) (input, output int64, ok bool) {
	var resp struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Usage == nil {
		return 0, 0, false
	}
	if resp.Usage.PromptTokens < 0 {
		resp.Usage.PromptTokens = 0
	}
	if resp.Usage.CompletionTokens < 0 {
		resp.Usage.CompletionTokens = 0
	}
	return resp.Usage.PromptTokens, resp.Usage.CompletionTokens, true
}
