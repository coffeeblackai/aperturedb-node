import { TokenCalculation } from '../../types/aperture';

// Constants for token calculations
const GEMINI_IMAGE_TOKENS = 258; // Fixed token cost for images in Gemini
const CHARS_PER_TOKEN = 4; // Approximate characters per token for text

// Pricing per million tokens
const INPUT_PRICE_PER_MILLION = 1.25;  // $1.25 per 1M input tokens
const OUTPUT_PRICE_PER_MILLION = 5.00;  // $5.00 per 1M output tokens

export function calculateTextTokens(text: string): number {
  return Math.ceil(text.length / CHARS_PER_TOKEN);
}

export function calculateImageTokens(): number {
  return GEMINI_IMAGE_TOKENS;
}

export function calculateTotalTokens(text: string, hasImage: boolean = false): number {
  const textTokens = calculateTextTokens(text);
  const imageTokens = hasImage ? GEMINI_IMAGE_TOKENS : 0;
  return textTokens + imageTokens;
}

export function calculateCost(inputTokens: number, outputTokens: number): TokenCalculation {
  const inputCost = (inputTokens / 1_000_000) * INPUT_PRICE_PER_MILLION;
  const outputCost = (outputTokens / 1_000_000) * OUTPUT_PRICE_PER_MILLION;
  return {
    inputCost,
    outputCost,
    totalCost: inputCost + outputCost
  };
} 