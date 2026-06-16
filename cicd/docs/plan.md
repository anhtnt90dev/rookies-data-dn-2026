

## 1. Context

**chưa có quy trình deploy tự động** nào cho Fabric workspace. Điều này dẫn đến:

- Deploy thủ công tốn thời gian, dễ xảy ra lỗi sót item
- Không có approval workflow trước khi đưa thay đổi lên Test/Prod
- Notebooks chứa hardcoded GUIDs (Lakehouse ID, Workspace ID) — khi copy sang môi trường khác phải sửa tay từng file
- Không có audit trail: không biết ai deploy gì, lúc nào, từ branch nào

---

## 2. Giải pháp đề xuất

Xây dựng CI/CD pipeline dựa trên **Azure DevOps (ADO)** + **thư viện `fabric-cicd`** của Microsoft.

### Kiến trúc tổng quan

```
Git branch (feature/*) 
    ↓ merge to main
ADO Pipeline trigger tự động
    ↓
fabric-cicd publish_all_items()
    ↓ (GUID auto-replace via parameter.yml)
Test workspace  ←  Approval gate (manual approve)
    ↓
Prod workspace  ←  Approval gate (manual approve)
```

### Các thành phần chính

| Thành phần | Vai trò | Ghi chú |
|---|---|---|
| `fabric-cicd` (Python lib) | Deploy Fabric items tự động | Microsoft-maintained, open source |
| `parameter.yml` | Map GUIDs dev → test/prod | Giải quyết vấn đề hardcoded IDs trong Notebook |
| ADO Pipeline YAML | Định nghĩa luồng CI/CD | Trigger on merge, stages, approvals |
| ADO Environments | Approval gate trước mỗi môi trường | Test & Prod cần người phê duyệt |
| Azure Key Vault + ADO Variable Groups | Quản lý credentials an toàn | Service Principal, connection strings |

---

## 3. Giải quyết vấn đề hardcoded GUIDs

Đây là vấn đề kỹ thuật đặc thù của Fabric. Khi Notebook dùng `%%configure` để attach Lakehouse, file định nghĩa sẽ lưu trực tiếp các ID như:

```json
{
  "defaultLakehouse": {
    "name": "lh_silver",
    "id": "xxxxxxxx-dev-lakehouse-id",
    "workspaceId": "xxxxxxxx-dev-workspace-id"
  }
}
```

`fabric-cicd` tự động thay thế các GUIDs này trước khi publish, dựa trên file `parameter.yml`:

```yaml
# parameter.yml
find_replace:
  - find: "xxxxxxxx-dev-workspace-id"
    replace_with:
      PPE: "yyyyyyyy-test-workspace-id"
      PROD: "zzzzzzzz-prod-workspace-id"
  - find: "xxxxxxxx-dev-lakehouse-id"
    replace_with:
      PPE: "yyyyyyyy-test-lakehouse-id"
      PROD: "zzzzzzzz-prod-lakehouse-id"
```

**Không cần sửa tay một dòng nào sau khi setup xong.**

---

## 4. Kế hoạch triển khai theo giai đoạn

### Giai đoạn 1 — Setup nền tảng 
**Mục tiêu:** Có pipeline chạy được từ dev lên Test

- [ ] Tạo Service Principal trong Azure AD, cấp quyền vào Fabric workspaces
- [ ] Setup Azure Key Vault, lưu credentials của Service Principal
- [ ] Tạo ADO Variable Group, liên kết với Key Vault
- [ ] Cài đặt `fabric-cicd`, viết script Python deploy cơ bản
- [ ] Thu thập GUIDs của cả 3 môi trường, viết `parameter.yml`
- [ ] Viết ADO Pipeline YAML với trigger on merge và 1 stage deploy lên Test

**Kết quả:** Push code → merge → pipeline tự deploy lên Test trong ~5 phút

---

### Giai đoạn 2 — Approval workflow & Prod 
**Mục tiêu:** Hoàn thiện luồng Test → Prod với approval gate

- [ ] Tạo ADO Environments: `fabric-test` và `fabric-prod`
- [ ] Cấu hình approval checks trên từng Environment (chỉ định người approve)
- [ ] Thêm stage Prod vào Pipeline YAML, gắn với `fabric-prod` environment
- [ ] Test toàn bộ luồng: dev → test (approve) → prod (approve)
- [ ] Viết runbook hướng dẫn sử dụng cho team

**Kết quả:** Luồng đầy đủ dev → test → prod với approval, có audit log trên ADO

---

### Giai đoạn 3 — Hardening & tối ưu
**Mục tiêu:** Ổn định, dễ maintain, sẵn sàng scale

- [ ] Thêm tham số `items_in_scope` để control loại item nào được deploy (Notebook, DataPipeline, SemanticModel…)
- [ ] Thêm bước smoke test tự động sau mỗi deploy
- [ ] Review và document danh sách GUIDs cần maintain
- [ ] Hướng dẫn teammate về quy trình làm việc mới với Git branches

**Kết quả:** Pipeline production-ready, team tự vận hành được

---

## 5. So sánh: Trước và sau

| Tiêu chí | Hiện tại (thủ công) | Sau khi có CI/CD |
|---|---|---|
| Thời gian deploy | 30–60 phút/lần | ~5 phút (tự động) |
| Sửa GUIDs | Tay, dễ sót | Tự động qua parameter.yml |
| Approval trước Prod | Không có | Bắt buộc qua ADO gate |
| Audit trail | Không có | Đầy đủ trên ADO (ai, khi nào, branch nào) |
| Khả năng rollback | Khó, không rõ ràng | Git history + ADO run history |
| Rủi ro lỗi human | Cao | Thấp |

---

---

## 6. Rủi ro và cách giảm thiểu

| Rủi ro | Khả năng | Cách giảm thiểu |
|---|---|---|
| Thiếu quyền Azure AD để tạo Service Principal | Trung bình | Xác nhận với IT/Azure admin ngay tuần 1 |
| GUIDs thay đổi khi workspace bị recreate | Thấp | Document và có checklist cập nhật parameter.yml |
| fabric-cicd không hỗ trợ một item type cụ thể | Thấp | Kiểm tra [compatibility list](https://microsoft.github.io/fabric-cicd/) trước khi commit |

---
