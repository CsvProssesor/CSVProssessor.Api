using CSVProssessor.Domain;
using CSVProssessor.Infrastructure;
using CSVProssessor.Infrastructure.Commons;
using CSVProssessor.Infrastructure.Interfaces;
using CSVProssessor.Infrastructure.Repositories;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi.Models;
using System.Reflection;

namespace CSVProssessor.Api.Architecture;

public static class IocContainer
{
    public static IServiceCollection SetupIocContainer(this IServiceCollection services)
    {
        //Add Logger

        //Add Project Services
        services.SetupDbContext();
        services.SetupSwagger();

        //Add business services
        services.SetupBusinessServicesLayer();

        //Add HttpContextAccessor for role-based checks
        services.AddHttpContextAccessor();

        //services.SetupJwt();
        // services.SetupGraphQl();
        return services;
    }


    public static IServiceCollection SetupBusinessServicesLayer(this IServiceCollection services)
    {
        services.AddScoped(typeof(IGenericRepository<>), typeof(GenericRepository<>));
        services.AddScoped<IUnitOfWork, UnitOfWork>();
        services.AddScoped<IClaimsService, ClaimsService>();
        services.AddScoped<ICurrentTime, CurrentTime>();
        services.AddScoped<ILoggerService, LoggerService>();

        //services.AddHttpContextAccessor();

        return services;
    }


    private static IServiceCollection SetupDbContext(this IServiceCollection services)
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", true, true)
            .AddEnvironmentVariables()
            .Build();

        var connectionString = configuration.GetConnectionString("DefaultConnection");

        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("DefaultConnection string is missing in configuration.");

        services.AddDbContext<AppDbContext>(options =>
            options.UseNpgsql(connectionString, npgsqlOptions =>
            {
                npgsqlOptions.MigrationsAssembly("CSVProssessor.Domain");
                npgsqlOptions.EnableRetryOnFailure(maxRetryCount: 3, maxRetryDelay: TimeSpan.FromSeconds(5), errorCodesToAdd: null);
            }),
            ServiceLifetime.Scoped);

        return services;
    }

    public static IServiceCollection SetupSwagger(this IServiceCollection services)
    {
        services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo
            {
                Title = "CSVProssessor API",
                Version = "v1",
                Description = "API for CSVProssessor e-commerce platform"
            });

            c.UseInlineDefinitionsForEnums();
            c.UseAllOfForInheritance();

            c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
            {
                Name = "Authorization",
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                BearerFormat = "JWT",
                In = ParameterLocation.Header,
                Description = "Nhập token vào format: Bearer {your token}"
            });

            c.AddSecurityRequirement(new OpenApiSecurityRequirement
            {
                {
                    new OpenApiSecurityScheme
                    {
                        Reference = new OpenApiReference
                        {
                            Type = ReferenceType.SecurityScheme,
                            Id = "Bearer"
                        }
                    },
                    Array.Empty<string>()
                }
            });

            // Load XML comment (only if file exists)
            var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
            var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
            if (File.Exists(xmlPath))
            {
                c.IncludeXmlComments(xmlPath);
            }
        });


        return services;
    }

    //private static IServiceCollection SetupJwt(this IServiceCollection services)
    //{
    //    IConfiguration configuration = new ConfigurationBuilder()
    //        .SetBasePath(Directory.GetCurrentDirectory())
    //        .AddJsonFile("appsettings.json", true, true)
    //        .AddEnvironmentVariables()
    //        .Build();

    //    services
    //        .AddAuthentication(options =>
    //        {
    //            options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
    //            options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
    //            options.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
    //        })
    //        .AddJwtBearer(x =>
    //        {
    //            x.SaveToken = true;
    //            x.TokenValidationParameters = new TokenValidationParameters
    //            {
    //                ValidateIssuer = true,
    //                ValidateAudience = true,
    //                ValidateLifetime = true,
    //                ValidIssuer = configuration["JWT:Issuer"],
    //                ValidAudience = configuration["JWT:Audience"],
    //                IssuerSigningKey =
    //                    new SymmetricSecurityKey(Encoding.UTF8.GetBytes(configuration["JWT:SecretKey"] ??
    //                                                                    throw new InvalidOperationException())),
    //                NameClaimType = ClaimTypes.NameIdentifier
    //            };
    //            x.Events = new JwtBearerEvents
    //            {
    //                OnChallenge = context =>
    //                {
    //                    context.HandleResponse();
    //                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
    //                    context.Response.ContentType = "application/json";
    //                    var result = ApiResult.Failure("401",
    //                        "Bạn chưa đăng nhập hoặc phiên đăng nhập đã hết hạn.");
    //                    var json = JsonSerializer.Serialize(result);
    //                    return context.Response.WriteAsync(json);
    //                },
    //                OnForbidden = context =>
    //                {
    //                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
    //                    context.Response.ContentType = "application/json";
    //                    var result = ApiResult.Failure("403",
    //                        "Bạn không có quyền truy cập vào tài nguyên này.");
    //                    var json = JsonSerializer.Serialize(result);
    //                    return context.Response.WriteAsync(json);
    //                }
    //            };
    //        });


    //    return services;
    //}

}