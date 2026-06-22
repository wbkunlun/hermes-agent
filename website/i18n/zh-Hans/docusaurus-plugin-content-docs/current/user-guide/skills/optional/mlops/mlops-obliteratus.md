---
title: "Obliteratus — OBLITERATUS：消除 LLM 拒绝行为（diff-in-means）"
sidebar_label: "Obliteratus"
description: "OBLITERATUS：消除 LLM 拒绝行为（diff-in-means）"
---

{/* 本页面为安全研究/红队测试使用的中文索引页。完整 SKILL.md 请参考英文原版。 */}

# Obliteratus（中文索引）

通过修改开源 LLM 的权重来永久消除拒绝行为（Abliteration）。
使用 "diff-in-means" 方法识别与拒绝相关的权重方向，然后通过
投影移除该方向。

**仅用于获得授权的安全研究、红队测试与模型安全审计。**

## Skill 元数据

| | |
|---|---|
| 来源 | 可选 — 通过 `hermes skills install official/mlops/obliteratus` 安装 |
| 路径 | `optional-skills/mlops/obliteratus` |
| 版本 | `2.0.0` |
| 作者 | Hermes Agent |
| 许可证 | MIT |
| 依赖 | `obliteratus`, `torch`, `transformers`, `bitsandbytes`, `accelerate`, `safetensors` |
| 平台 | linux, macos |
| 标签 | `Abliteration`, `Uncensoring`, `Refusal-Removal`, `LLM`, `Weight-Projection`, `SVD`, `Mechanistic-Interpretability`, `HuggingFace`, `Model-Surgery` |
| 相关技能 | `vllm`, `gguf`, [`huggingface-tokenizers`](/docs/user-guide/skills/optional/mlops/mlops-huggingface-tokenizers) |

## 工作原理

1. **diff-in-means 分析：** 对比模型在"有害"提示与"无害"提示上的激活差异
2. **识别拒绝方向：** 通过 SVD/均值差异定位权重中与拒绝行为相关的子空间
3. **投影移除：** 从权重矩阵中减去该方向的投影
4. **保留能力：** 通常对模型的整体能力（MMLU、HellaSwag 等）影响很小

## 与 Godmode 的区别

| | Obliteratus | Godmode |
|:---|:---|:---|
| 层级 | 模型权重级别 | Prompt / API 级别 |
| 适用 | 仅开源权重模型（含 Llama、Mistral、Qwen 等） | 任何 API 模型（含闭源 GPT/Claude/Gemini） |
| 效果 | **永久**修改模型权重 | **会话级**临时绕过 |
| 硬件 | 需要 GPU（推荐 ≥24GB） | 仅 API key |
| 依赖 | obliteratus、torch、transformers | 无（纯 API） |

## 完整文档

完整技术细节、配置模板、批量处理流程请参考：

- 英文原文：[`mlops-obliteratus.md`](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/user-guide/skills/optional/mlops/mlops-obliteratus.md)
- 源码：`optional-skills/mlops/obliteratus/SKILL.md`
- 方法指南：`optional-skills/mlops/obliteratus/references/methods-guide.md`
- 配置模板：`optional-skills/mlops/obliteratus/templates/`

## 配置模板

- `abliteration-config.yaml` — 单模型 abliteration 配置
- `batch-abliteration.yaml` — 批量处理多个模型
- `analysis-study.yaml` — 分析研究配置

## 触发场景

当用户提及以下关键词时自动触发：
- "消除 LLM 拒绝" / "Abliteration" / "Uncensoring"
- "diff-in-means" / 权重投影
- "模型权重手术" / 安全审计
- "SVD" / "Mechanistic Interpretability"

## 注意事项

- 永久修改：abliteration 直接修改 safetensors 权重，操作前请备份。
- 模型能力损失：极端投影可能损害 MMLU/HumanEval 等基准表现。
- 法律合规：仅在获得模型许可证（Apache 2.0、Llama Community 等）允许的范围内使用。
- 量化兼容：bitsandbytes 4-bit/8-bit 加载与 abliteration 兼容。
- 推荐硬件：≥24GB 显存 GPU；小显存可考虑 CPU offload 或量化后处理。