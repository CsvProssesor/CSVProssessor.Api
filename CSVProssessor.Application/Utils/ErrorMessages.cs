namespace CSVProssessor.Application.Utils;

public static class ErrorMessages
{
    #region Account Error Message

    public const string AccountNotFound = "Không tìm thấy tài khoản với email này.";
    public const string AccountNotVerified = "Email chưa được xác thực.";
    public const string AccountEmailAlreadyRegistered = "Email này đã được đăng ký.";
    public const string AccountSuspendedOrBan = "Tài khoản đã bị tạm khóa hoặc cấm sử dụng.";
    public const string AccountAlreadyVerified = "Email đã được xác thực.";

    #endregion Account Error Message

    #region Caching

    public const string VerifyOtpExistingCoolDown =
        "Bạn đang gửi mã OTP quá nhanh. Vui lòng chờ vài phút trước khi thử lại.";

    public const string CacheUserNotFound = "Không tìm thấy thông tin người dùng.";

    #endregion Caching

    #region Oauth Error Message

    public const string Oauth_ClientIdMissing = "Thiếu thông tin Client ID.";
    public const string Oauth_InvalidToken = "Token Google không hợp lệ.";
    public const string Oauth_PayloadNull = "Dữ liệu từ Google bị lỗi.";
    public const string Oauth_InvalidCredential = "Thông tin đăng nhập không hợp lệ.";
    public const string Oauth_InvalidOtp = "Mã OTP không chính xác.";

    #endregion Oauth Error Message
}