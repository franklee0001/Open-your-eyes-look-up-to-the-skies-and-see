# Cloudflare Access (이메일 OTP) 설정 가이드

이 문서는 GitHub Pages 정적 리포트를 **Cloudflare Access 이메일 OTP**로 보호하는 방법을 설명합니다.
정적 사이트는 클라이언트 측 비밀번호로는 완전한 보안이 되지 않으므로, 반드시 Cloudflare Access로 게이트를 적용합니다.

## 1) 도메인 준비 + Cloudflare 연결
1. 도메인을 구매합니다.
2. Cloudflare에 도메인을 추가합니다.
3. 도메인 등록기관에서 네임서버를 Cloudflare에서 안내하는 값으로 변경합니다.

## 2) GitHub Pages 커스텀 도메인 설정
1. GitHub 리포지토리 → Settings → Pages로 이동합니다.
2. Custom domain에 `report.example.com` 같은 서브도메인을 입력합니다.
3. GitHub가 안내하는 DNS 레코드를 Cloudflare DNS에 추가합니다.
   - 보통 CNAME 레코드(예: `report` → `username.github.io`) 형태입니다.
4. Cloudflare에서 해당 DNS 레코드를 **Proxy ON (오렌지 구름)**으로 설정합니다.

## 3) Cloudflare Access 애플리케이션 생성
1. Cloudflare Zero Trust 콘솔 → Access → Applications로 이동합니다.
2. **Add an application** → **Self-hosted**를 선택합니다.
3. Application hostname에 `report.example.com`을 입력합니다.
4. 로그인 방식은 **One-time PIN (이메일 OTP)**을 사용합니다.

## 4) 접근 정책(Allowlist) 설정
1. Access → Policies에서 **Allow** 정책을 추가합니다.
2. 이메일 조건으로 허용 목록(정확히 3명)을 등록합니다.
3. 기본 정책은 **Deny**로 유지합니다.

## 5) 동작 확인
1. 시크릿/시크릿 탭(로그아웃 상태)에서 `https://report.example.com` 접속합니다.
2. 이메일 입력 후 OTP가 수신되고 인증이 되는지 확인합니다.
3. 허용되지 않은 이메일은 차단되는지 확인합니다.

## 6) 기대되는 동작
- 허용된 3명만 이메일 OTP로 리포트 접근 가능
- GitHub Actions 일정 실행은 그대로 동작
- Pages는 정적 배포 유지, Cloudflare Access가 실접속을 차단/허용
