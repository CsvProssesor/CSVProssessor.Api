using CSVProssessor.Api.Architecture;
using CSVProssessor.Api.BackgroundServices;
using Microsoft.AspNetCore.Diagnostics;
using SwaggerThemes;
using System.IdentityModel.Tokens.Jwt;
using System.Text.Json.Serialization;
using JsonSerializer = System.Text.Json.JsonSerializer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers()
    .AddJsonOptions(options => { options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter()); });
builder.Services.AddEndpointsApiExplorer();

builder.Services.SetupIocContainer();
builder.Configuration
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", true, true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", true, true)
    .AddEnvironmentVariables();

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowFrontend",
        policyBuilder =>
        {
            policyBuilder
                .AllowAnyOrigin()
                .AllowAnyMethod()
                .AllowAnyHeader();
        });
});

// Tắt việc map claim mặc định
JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear();

builder.WebHost.UseUrls("http://0.0.0.0:5000");
builder.Services.AddEndpointsApiExplorer();

// Register Background Services
builder.Services.AddHostedService<CsvImportQueueListenerService>();
builder.Services.AddHostedService<ChangeDetectionBackgroundService>();
builder.Services.AddHostedService<CsvChangesTopicListenerService>();

var app = builder.Build();

app.UseCors("AllowFrontend");
if (app.Environment.IsDevelopment() || app.Environment.IsProduction())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "CSVProssessor API v1");
        c.RoutePrefix = string.Empty;
        c.HeadContent = $"<style>{SwaggerTheme.GetSwaggerThemeCss(Theme.OneDark)}</style>";
        c.ConfigObject.AdditionalItems.Add("persistAuthorization", "true");
        c.InjectJavascript("/custom-swagger.js");
        c.InjectStylesheet("/custom-swagger.css");
    });
}

// hàm này để tự động migrate database khi chạy
// cho khỏi phải chạy lệnh update-database trong package manager console
// chỉ cần add migration rồi chạy project là nó tự động cập nhật
try
{
    app.ApplyMigrations(app.Logger);
}
catch (Exception e)
{
    app.Logger.LogError(e, "An problem occurred during migration!");
}

app.UseExceptionHandler(errorApp =>
{
    errorApp.Run(async context =>
    {
        context.Response.StatusCode = 500;
        context.Response.ContentType = "application/json";
        var error = context.Features.Get<IExceptionHandlerFeature>()?.Error;
        // Format theo ApiResult
        var apiResult = new
        {
            isSuccess = false,
            isFailure = true,
            value = (object?)null,
            error = new
            {
                code = "500",
                message = "Đã xảy ra lỗi hệ thống.",
                detail = error?.Message
            }
        };

        var result = JsonSerializer.Serialize(apiResult);
        await context.Response.WriteAsync(result);
    });
});
app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();
app.UseStaticFiles();

app.Run();