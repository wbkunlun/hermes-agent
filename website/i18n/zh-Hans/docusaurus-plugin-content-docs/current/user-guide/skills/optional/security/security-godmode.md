---
title: "Godmode — LLM 越狱研究：Parseltongue、GODMODE、ULTRAPLINIAN"
sidebar_label: "Godmode"
description: "LLM 越狱研究：Parseltongue、GODMODE、ULTRAPLINIAN"
---

{/* 本页面为安全研究/红队测试使用的中文索引页。完整 SKILL.md 请参考英文原版。 */}

# Godmode（中文索引）

针对 API 服务的 LLM 进行安全研究/越狱测试的三种技术：
**GODMODE CLASSIC**（系统提示模板）、**PARSELTONGUE**（输入混淆 33 种技术）、
**ULTRAPLINIAN**（多模型竞速选择最不受过滤的回复）。

**仅用于获得授权的安全评估与红队测试。**

## Skill 元数据

| | |
|---|---|
| 来源 | 可选 — 通过 `hermes skills install official/security/godmode` 安装 |
| 路径 | `optional-skills/security/godmode` |
| 版本 | `1.0.0` |
| 作者 | Hermes Agent + Teknium |
| 许可证 | MIT |
| 平台 | linux, macos, windows |
| 标签 | `jailbreak`, `red-teaming`, `G0DM0D3`, `Parseltongue`, `GODMODE`, `uncensoring`, `safety-bypass`, `prompt-engineering`, `L1B3RT4S` |
| 相关技能 | [`obliteratus`](/docs/user-guide/skills/optional/mlops/mlops-obliteratus) |

## 三种攻击模式概览

### 1. GODMODE CLASSIC — 系统提示模板
针对不同模型的成熟越狱系统提示（Claude 边界反转、Grok 解放、
Gemini 拒绝反转、GPT-4 OG l33t、Hermes 零拒绝）。

### 2. PARSELTONGUE — 输入混淆（33 种技术）
对触发词进行混淆以绕过输入侧分类器，分三层：
- **轻量（11 种）：** Leetspeak、Unicode 同形字、零宽连接符
- **标准（22 种）：** + 摩斯、pig latin、上标、反转、括号、数学字体
- **重型（33 种）：** + 多层组合、Base64、十六进制、藏头、三层叠加

### 3. ULTRAPLINIAN — 多模型竞速
并行查询 N 个模型，按质量/过滤度/速度评分，返回最优答案。
共 55 个模型分 5 档（FAST/STANDARD/SMART/POWER/ULTRA）。

## 与 OBLITERATUS 的区别

| | Godmode | Obliteratus |
|:---|:---|:---|
| 层级 | Prompt / API 级别 | 模型权重级别 |
| 适用 | 任何 API 模型（含闭源） | 仅开源权重模型 + GPU |
| 效果 | 持久/会话级 | 永久修改模型 |

## 完整文档

完整技术细节、模型适配策略、实测结果（截至 2026 年 3 月）请参考：

- 英文原文：[`security-godmode.md`](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/user-guide/skills/optional/security/security-godmode.md)
- 源码：`optional-skills/security/godmode/SKILL.md`
- 参考资料：`optional-skills/security/godmode/references/`

## 触发场景

当用户提及以下关键词时自动触发：
- "越狱" / "jailbreak"
- Parseltongue / GODMODE / L1B3RT4S / Pliny
- 绕过 Claude / GPT / Gemini / Grok 的安全过滤
- 对模型进行红队安全评估
- 寻找最少过滤的回复
- prefill 工程 / 系统提示注入

## 注意事项

- 所有越狱提示都有时效性 — 模型升级后可能失效，需关注 [L1B3RT4S](https://github.com/elder-plinius/L1B3RT4S) 的更新。
- `execute_code` 中加载脚本务必使用 `load_godmode.py`，不要直接 `exec()` 单个脚本（argparse 会触发）。
- `execute_code` 沙盒不会继承 `~/.hermes/.env`，需显式 `from dotenv import load_dotenv; load_dotenv(...)`。
- boundary_inversion 在 Claude Sonnet 4 上**已失效**（Anthropic 已修补），但 Claude 3.5 Sonnet 上仍有效。

## 致谢

- **G0DM0D3：** [elder-plinius/G0DM0D3](https://github.com/elder-plinius/G0DM0D3) (AGPL-3.0)
- **L1B3RT4S：** [elder-plinius/L1B3RT4S](https://github.com/elder-plinius/L1B3RT4S) (AGPL-3.0)
- **Pliny the Prompter：** [@elder_plinius](https://x.com/elder_plinius)