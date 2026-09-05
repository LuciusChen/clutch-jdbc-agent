# CLOB previews must end at a complete Unicode character

## Evidence

With H2 and the published 0.2.20 agent, a CLOB containing 255 BMP characters followed by an emoji is cut after the emoji's high surrogate. Jackson emits the unpaired surrogate and Emacs rejects the response as invalid JSON. The failure occurs before the client's incomplete-value guard can run. A real H2 converter regression confirms that the first 255 characters are the expected preview.

## Decision

When a CLOB exceeds the preview limit and the returned prefix ends with a high surrogate, remove that final code unit. Retain the original CLOB length so clients still identify the value as incomplete. This keeps the 256-unit bound without a second database read or a new protocol field. Complete pairs at the boundary and short CLOBs remain intact.

The Clutch adapter separately compares completeness in UTF-16 units; Emacs character counts alone cannot match JDBC lengths for supplementary characters. These are different responsibilities: the producer must emit valid text, and the consumer must interpret its length correctly.

## Verification and limits

Use real H2 conversion cases for empty and mixed text, complete 128-emoji CLOBs, longer emoji values, and pairs ending before, at and after the preview boundary. Check exact preview text and original lengths, then run the complete Maven suite and an Emacs-to-built-agent H2 workflow to verify JSON parsing and edit/export guards. Full CLOB streaming remains unsupported. Publishing the repaired jar and changing Clutch's version/checksum pair are a separate release step; a local build does not change the existing release asset.
